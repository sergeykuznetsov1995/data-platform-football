import hashlib
import json
from dataclasses import replace
from datetime import datetime, timezone

import pytest

from scrapers.transfermarkt.achievement_observations import (
    ACHIEVEMENT_OBSERVATION_COLUMNS,
    ACHIEVEMENT_OBSERVATION_KEY,
    AchievementObservationError,
    AchievementRecipient,
    AchievementUniverse,
    achievement_url,
    extract_achievement_capture,
    validate_achievement_extraction,
)
from scrapers.transfermarkt.raw_store import RawCaptureRecord


@pytest.mark.parametrize(
    ("kind", "slug", "expected"),
    [
        ("player", None, "https://tmapi.transfermarkt.technology/player/7/achievement"),
        ("club", None, "https://tmapi.transfermarkt.technology/club/7/achievement"),
        ("coach", "test-coach", "https://www.transfermarkt.com/test-coach/erfolge/trainer/7"),
    ],
)
def test_achievement_url(kind, slug, expected):
    assert achievement_url(recipient_type=kind, recipient_id="007", slug=slug) == expected


@pytest.mark.parametrize(
    ("kind", "identifier", "slug"),
    [
        ("team", 1, None), ("club", 0, None), ("player", "000", None),
        ("coach", 7, None), ("coach", 7, ""), ("coach", 7, "../test"),
        ("coach", 7, "a/b"), ("coach", 7, "a?b"), ("coach", 7, "a#b"),
        ("coach", 7, "%2e%2e"), ("coach", 7, 1), ("club", 1, "bad/path"),
        ("club", True, None), ("player", 1.5, None), ("player", "-1", None),
        ("player", "+1", None), ("player", "1a", None), ("player", "١", None),
    ],
)
def test_invalid_url_identity(kind, identifier, slug):
    with pytest.raises(AchievementObservationError):
        achievement_url(recipient_type=kind, recipient_id=identifier, slug=slug)


def test_manifest_binds_version_and_full_sorted_inventory():
    player = AchievementRecipient("player", 7)
    coach = AchievementRecipient("coach", 7, "test-coach")
    manifest = AchievementUniverse("v1", (player, coach))
    assert manifest.recipients == (coach, player)
    assert manifest.assessment_manifest_id == AchievementUniverse(
        "v1", (coach, player),
    ).assessment_manifest_id
    others = [
        AchievementUniverse("v1", (coach,)),
        AchievementUniverse("v2", (player, coach)),
        AchievementUniverse("v1", (player, replace(coach, slug="second-coach"))),
        AchievementUniverse("v1", (player, replace(coach, recipient_id=8))),
        AchievementUniverse("v1", (AchievementRecipient("club", 7), coach)),
    ]
    assert all(other.assessment_manifest_id != manifest.assessment_manifest_id for other in others)


@pytest.mark.parametrize("second_slug", ["test-coach", "second-coach"])
def test_manifest_rejects_duplicate_and_conflicting_identity(second_slug):
    with pytest.raises(AchievementObservationError):
        AchievementUniverse("v1", (
            AchievementRecipient("coach", 7, "test-coach"),
            AchievementRecipient("coach", "007", second_slug),
        ))


@pytest.mark.parametrize("version", ["", " ", None, 1])
def test_manifest_requires_version(version):
    with pytest.raises(AchievementObservationError):
        AchievementUniverse(version, (AchievementRecipient("player", 7),))


@pytest.mark.parametrize("recipients", [(), (None,), "player/7"])
def test_manifest_requires_recipient_objects(recipients):
    with pytest.raises(AchievementObservationError):
        AchievementUniverse("v1", recipients)


def _capture(body, kind="player", **overrides):
    slug = "test-coach" if kind == "coach" else None
    recipient = AchievementRecipient(kind, 7, slug)
    values = {
        "manifest_version": "transfermarkt-raw-v1",
        "capture_id": "a" * 64,
        "source": "transfermarkt",
        "cycle_id": "cycle-1",
        "scope_id": f"achievement:{kind}:7",
        "endpoint": f"{kind}_achievement",
        "attempt": 1,
        "url": achievement_url(**recipient.inventory_entry()),
        "status_code": 200,
        "headers": {},
        "content_type": "text/html" if kind == "coach" else "application/json",
        "content_hash": hashlib.sha256(body).hexdigest(),
        "hash_algorithm": "sha256",
        "blob_key": "blobs/body.gz",
        "raw_uri": "file:///raw/blobs/body.gz",
        "compression": "gzip",
        "fetched_at": "2026-11-20T10:00:00+00:00",
        "decoded_bytes": len(body),
        "stored_bytes": len(body),
    }
    values.update(overrides)
    return recipient, RawCaptureRecord(**values)


def _extract(body, kind="player", **overrides):
    recipient, raw = _capture(body, kind, **overrides)
    universe = AchievementUniverse("registry-test", (recipient,))
    return extract_achievement_capture(
        body,
        raw,
        recipient=recipient,
        universe=universe,
        run_id="run-1",
        extracted_at=datetime(2026, 11, 20, 11, tzinfo=timezone.utc),
    )


def _player_body(rows):
    return json.dumps({
        "success": True,
        "data": {
            "aggregated": [{"id": 17, "receivedCount": len(rows), "name": "Cup"}],
            "achievements": rows,
        },
    }).encode()


def _player_row():
    return {
        "id": 17,
        "trophyId": 31,
        "seasonId": 2022,
        "season": {"id": 2022, "display": "2022"},
        "details": {"playerId": 7, "clubId": 61, "competitionId": "Q1"},
    }


def test_full_player_capture_preserves_repeated_details_and_lineage():
    body = _player_body([_player_row(), _player_row()])
    first = _extract(body)
    second = _extract(body)
    assert first.canonical_json() == second.canonical_json()
    assert first.plan_sha256() == second.plan_sha256()
    assert json.loads(first.canonical_json())["rows"] == [dict(row) for row in first.rows]
    assert len(first.rows) == 2
    assert len({tuple(row[key] for key in ACHIEVEMENT_OBSERVATION_KEY) for row in first.rows}) == 2
    assert [row["source_row_id"] for row in first.rows] == ["achievements:0", "achievements:1"]
    row = first.rows[0]
    assert set(row) == set(ACHIEVEMENT_OBSERVATION_COLUMNS)
    assert row["recipient_type"] == "player" and row["recipient_id"] == "7"
    assert row["title"] == "Cup" and row["season"] == "2022" and row["season_id"] == "2022"
    assert row["award_id"] == "17" and row["trophy_id"] == "31"
    assert row["club_id"] == "61" and row["competition_id"] == "Q1"
    assert row["observation_status"] == "observed_present"
    assert row["source_capture_id"] == row["raw_capture_id"] == "a" * 64
    assert row["source_body_hash"] == hashlib.sha256(body).hexdigest()
    assert row["source_url"] == "https://tmapi.transfermarkt.technology/player/7/achievement"
    assert row["source_endpoint"] == "player_achievement"
    assert row["scope_id"] == "achievement:player:7" and row["cycle_id"] == "cycle-1"
    assert row["captured_at"] == row["observed_at"] == "2026-11-20T10:00:00+00:00"
    assert row["extracted_at"] == "2026-11-20T11:00:00+00:00"
    assert row["run_id"] == "run-1" and row["universe_version"] == "registry-test"
    assert row["assessment_manifest_id"] == first.assessment_manifest_id
    assert row["parser_revision"] == "achievements-v1" and row["schema_revision"] == "1"
    assert validate_achievement_extraction(first) == []


def test_club_trophy_fallback_survives_capture_extraction():
    body = json.dumps({
        "trophies": [{"id": 6, "name": "Champions League Participant"}],
        "achievements": [{
            "id": 6,
            "trophyId": 6,
            "season": {"id": 2025, "display": "25/26"},
        }],
    }).encode()
    result = _extract(body, "club")
    row, = result.rows
    assert row["title"] == "Champions League Participant"
    assert row["season"] == "25/26" and row["trophy_id"] == "6"


def test_coach_capture_uses_saved_html_and_source_identity():
    body = (
        b'<div class="box"><h2 class="content-box-headline">1x Cup</h2><table>'
        b'<tr><td>2022</td><td><a href="/club/startseite/verein/21">Club</a></td></tr>'
        b'</table></div>'
    )
    row, = _extract(body, "coach").rows
    assert row["title"] == "Cup" and row["season"] == "2022"
    assert row["club_id"] == "21" and row["club_name"] == "Club"
    assert row["source_url"] == "https://www.transfermarkt.com/test-coach/erfolge/trainer/7"
    assert row["source_endpoint"] == "coach_achievement"


def test_valid_empty_json_emits_one_status_row_not_an_aggregate():
    result = _extract(b'{"achievements":[]}')
    row, = result.rows
    assert row["observation_status"] == "observed_empty"
    assert row["source_row_id"] == "recipient"
    for column in (
        "title", "award_id", "trophy_id", "season_id", "season", "result",
        "club_id", "club_name", "competition_id",
    ):
        assert row[column] is None


def test_extraction_rejects_wrong_raw_identity_and_hash():
    body = _player_body([_player_row()])
    for overrides in (
        {"status_code": 201},
        {"endpoint": "current_players"},
        {"url": "https://tmapi.transfermarkt.technology/player/8/achievement"},
        {"content_hash": "0" * 64},
    ):
        with pytest.raises(AchievementObservationError):
            _extract(body, **overrides)
    with pytest.raises(AchievementObservationError, match="invalid raw capture"):
        recipient, raw = _capture(body)
        extract_achievement_capture(
            body.decode(), raw, recipient=recipient,
            universe=AchievementUniverse("v1", (recipient,)), run_id="run",
            extracted_at=datetime.now(timezone.utc),
        )


def test_invalid_bodies_and_late_detail_fail_whole_extraction():
    for body, kind in (
        (b"\xff", "player"),
        (b"not-json", "player"),
        (_player_body([_player_row(), None]), "player"),
        (b"<html>no titles</html>", "coach"),
    ):
        with pytest.raises(AchievementObservationError):
            _extract(body, kind)


def test_recipient_must_belong_to_frozen_universe():
    body = _player_body([_player_row()])
    recipient, raw = _capture(body)
    other = AchievementRecipient("player", 8)
    with pytest.raises(AchievementObservationError, match="outside universe"):
        extract_achievement_capture(
            body, raw, recipient=recipient,
            universe=AchievementUniverse("v1", (other,)), run_id="run",
            extracted_at=datetime.now(timezone.utc),
        )


def test_run_and_source_or_extraction_timestamps_are_validated():
    body = _player_body([_player_row()])
    recipient, raw = _capture(body)
    universe = AchievementUniverse("v1", (recipient,))
    for run_id, extracted_at in (
        ("", datetime.now(timezone.utc)),
        ("run", datetime.now()),
        ("run", "not-a-date"),
    ):
        with pytest.raises(AchievementObservationError):
            extract_achievement_capture(
                body, raw, recipient=recipient, universe=universe,
                run_id=run_id, extracted_at=extracted_at,
            )
    naive_raw = replace(raw, fetched_at="2026-11-20T10:00:00")
    with pytest.raises(AchievementObservationError, match="fetched_at"):
        extract_achievement_capture(
            body, naive_raw, recipient=recipient, universe=universe,
            run_id="run", extracted_at=datetime.now(timezone.utc),
        )


def test_structural_validator_catches_duplicate_key_and_lineage_drift():
    extraction = _extract(_player_body([_player_row()]))
    row = dict(extraction.rows[0])
    duplicate = replace(extraction, rows=(row, row))
    assert any("duplicate observation key" in error for error in validate_achievement_extraction(duplicate))
    null_lineage = dict(row) | {"source_url": None}
    assert any("unexpected source_url" in error for error in validate_achievement_extraction(
        replace(extraction, rows=(null_lineage,))
    ))
    wrong_parser = dict(row) | {"parser_revision": "other"}
    assert any("unexpected parser_revision" in error for error in validate_achievement_extraction(
        replace(extraction, rows=(wrong_parser,))
    ))
    wrong_status = dict(row) | {"observation_status": "unknown"}
    assert any("invalid observation_status" in error for error in validate_achievement_extraction(
        replace(extraction, rows=(wrong_status,))
    ))
    missing_column = {key: value for key, value in row.items() if key != "title"}
    assert any("incorrect columns" in error for error in validate_achievement_extraction(
        replace(extraction, rows=(missing_column,))
    ))
