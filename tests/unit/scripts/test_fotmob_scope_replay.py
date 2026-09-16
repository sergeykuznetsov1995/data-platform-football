from __future__ import annotations

import gzip
import hashlib
import json
import zlib
from pathlib import Path

import pytest

from scripts import fotmob_scope_replay as mod


def _store_payload(
    raw_root: Path,
    canonical_url: str,
    payload: object,
    *,
    fetched_at: str = "2026-08-08T10:00:00+00:00",
    validated_at: str | None = None,
) -> str:
    target_key = hashlib.sha256(canonical_url.encode("utf-8")).hexdigest()
    body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
    content_hash = hashlib.sha256(body).hexdigest()
    blob_key = f"blobs/sha256/{content_hash[:2]}/{content_hash}.json.gz"
    blob_path = raw_root / blob_key
    blob_path.parent.mkdir(parents=True, exist_ok=True)
    compressed = gzip.compress(body, mtime=0)
    blob_path.write_bytes(compressed)
    manifest = {
        "manifest_version": "fotmob-raw-v1",
        "source": "fotmob",
        "target_key": target_key,
        "canonical_url": canonical_url,
        "content_hash": content_hash,
        "hash_algorithm": "sha256",
        "blob_key": blob_key,
        "raw_uri": f"file://{raw_root}/{blob_key}",
        "compression": "gzip",
        "fetched_at": fetched_at,
        "etag": None,
        "last_modified": None,
        "decoded_bytes": len(body),
        "source_encoded_bytes": len(body),
        "stored_bytes": len(compressed),
        "validated_at": validated_at or fetched_at,
    }
    manifest_path = (
        raw_root / "targets" / "sha256" / target_key[:2] / f"{target_key}.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    manifest_path.write_text(json.dumps(manifest), encoding="utf-8")
    return content_hash


def _store_profile(
    raw_root: Path,
    competition_id: int,
    name: str,
    *,
    gender: str = "male",
    competition_type: str = "league",
    age_group: str = "adult",
    seasons: tuple[str, ...] = ("2025",),
    host: str = "www.fotmob.com",
    fetched_at: str = "2026-08-08T10:00:00+00:00",
    validated_at: str | None = None,
) -> None:
    canonical_url = f"https://{host}/api/data/leagues?id={competition_id}"
    payload = {
        "details": {
            "id": competition_id,
            "name": name,
            "gender": gender,
            "type": competition_type,
            "ageGroup": age_group,
            "selectedSeason": seasons[0],
        },
        "allAvailableSeasons": list(seasons),
    }
    _store_payload(
        raw_root,
        canonical_url,
        payload,
        fetched_at=fetched_at,
        validated_at=validated_at,
    )


def _store_catalog(raw_root: Path, competitions: list[dict[str, object]]) -> str:
    return _store_payload(
        raw_root,
        "https://www.fotmob.com/api/data/allLeagues",
        {"countries": [{"name": "Test", "leagues": competitions}]},
    )


def _fixture_root(tmp_path: Path) -> Path:
    raw_root = tmp_path / "raw"
    # The same presentation name is intentionally used for a male and a female
    # competition. Structural source metadata, not the name, must decide.
    _store_profile(
        raw_root,
        1,
        "Premier Cup",
        seasons=("2025 Apertura", "2024 Clausura"),
    )
    _store_profile(raw_root, 2, "Premier Cup", gender="female")
    _store_profile(raw_root, 3, "Club Friendlies", competition_type="friendly")
    _store_profile(
        raw_root, 4, "National Team Friendlies", competition_type="friendly"
    )
    _store_profile(raw_root, 5, "UEFA U21 Championship")
    _store_profile(raw_root, 6, "Premier Reserve League")
    _store_profile(raw_root, 7, "Charity Match")
    _store_profile(raw_root, 11, "Player Testimonials")
    _store_profile(raw_root, 12, "Preseason Exhibitions")
    catalog_hash = _store_catalog(
        raw_root,
        [
            {"id": 1, "name": "Premier Cup"},
            {"id": 2, "name": "Premier Cup"},
            {"id": 3, "name": "Club Friendlies"},
            {"id": 4, "name": "National Team Friendlies"},
            {"id": 5, "name": "UEFA U21 Championship"},
            {"id": 6, "name": "Premier Reserve League"},
            {"id": 7, "name": "Charity Match"},
            {"id": 8, "name": "Timeout Cup"},
            {"id": 9, "name": "Missing Cup"},
            {"id": 10, "name": "Unobserved Cup"},
            {"id": 11, "name": "Player Testimonials"},
            {"id": 12, "name": "Preseason Exhibitions"},
        ],
    )
    catalog_url = "https://www.fotmob.com/api/data/allLeagues"
    observations = {
        "schema_version": "fotmob-profile-observations-v1",
        "catalog_target_key": hashlib.sha256(catalog_url.encode()).hexdigest(),
        "catalog_content_hash": catalog_hash,
        "observations": [
            {
                "competition_id": 8,
                "catalog_name": "Timeout Cup",
                "outcome": "timeout",
                "profile_target_key": hashlib.sha256(
                    b"https://www.fotmob.com/api/data/leagues?id=8"
                ).hexdigest(),
                "attempt_id": "probe-8-1",
                "attempts": 1,
                "source_validated": False,
                "stale": False,
                "observed_at": "2026-08-08T10:00:00+00:00",
            },
            {
                "competition_id": 9,
                "catalog_name": "Missing Cup",
                "outcome": "not_found",
                "http_status": 404,
                "profile_target_key": hashlib.sha256(
                    b"https://www.fotmob.com/api/data/leagues?id=9"
                ).hexdigest(),
                "attempt_id": "probe-9-1",
                "attempts": 1,
                "source_validated": True,
                "stale": False,
                "observed_at": "2026-08-08T10:00:00+00:00",
            },
            {
                "competition_id": 9,
                "catalog_name": "Missing Cup",
                "outcome": "not_found",
                "http_status": 404,
                "profile_target_key": hashlib.sha256(
                    b"https://www.fotmob.com/api/data/leagues?id=9"
                ).hexdigest(),
                "attempt_id": "probe-9-2",
                "attempts": 1,
                "source_validated": True,
                "stale": False,
                "observed_at": "2026-08-08T11:00:00+00:00",
            },
        ]
    }
    (raw_root / "profile-observations.json").write_text(
        json.dumps(observations), encoding="utf-8"
    )
    return raw_root


def test_replay_uses_structural_policy_and_builds_exact_contract(tmp_path):
    raw_root = _fixture_root(tmp_path)
    report = mod.build_replay_report(raw_root)

    assert report["schema_version"] == "fotmob-scope-replay-v1"
    assert report["classifier_version"] == "fotmob-men-v1"
    assert report["summary"]["catalog_id_count"] == 12
    assert report["summary"]["decisions"] == {
        "excluded": 7,
        "included": 3,
        "pending_probe": 2,
        "review_required": 0,
    }
    assert report["summary"]["structural_gender"] == {
        "blank": 0,
        "female": 1,
        "male": 8,
    }
    by_id = {item["competition_id"]: item for item in report["decisions"]}
    assert by_id[1]["decision"] == "included"
    assert by_id[2]["decision"] == "excluded"
    assert by_id[3]["decision"] == "included"
    assert by_id[4]["decision"] == "included"
    assert by_id[5]["policy_rule"] == "exclude_youth"
    assert by_id[6]["policy_rule"] == "exclude_reserve"
    assert by_id[7]["policy_rule"] == "exclude_show"
    assert by_id[8]["decision"] == "pending_probe"
    assert by_id[9]["policy_rule"] == "exclude_dead_profile"
    assert by_id[10]["policy_rule"] == "probe_required"
    assert by_id[11]["policy_rule"] == "exclude_show"
    assert by_id[12]["policy_rule"] == "exclude_show"
    assert report["pending_inventory"] == [by_id[8], by_id[10]]
    assert report["review_inventory"] == []

    contract = report["catalog_contract"]
    catalog_url = "https://www.fotmob.com/api/data/allLeagues"
    catalog_key = hashlib.sha256(catalog_url.encode()).hexdigest()
    catalog_manifest = json.loads(
        (
            raw_root
            / "targets"
            / "sha256"
            / catalog_key[:2]
            / f"{catalog_key}.json"
        ).read_text(encoding="utf-8")
    )
    assert report["catalog_content_hash"] == catalog_manifest["content_hash"]
    assert contract["classifier_version"] == "fotmob-men-v1"
    assert contract["included_ids"] == [1, 3, 4]
    assert contract["scopes"] == ["1=2025 Apertura", "3=2025", "4=2025"]
    assert report["included_ids_sha256"] == contract["included_ids_sha256"]


def test_replay_rejects_corrupt_or_duplicate_profile_identity(tmp_path):
    raw_root = _fixture_root(tmp_path)
    manifest = next((raw_root / "targets").rglob("*.json"))
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["target_key"] = "0" * 64
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(mod.ReplayError, match="target key"):
        mod.build_replay_report(raw_root)


def test_replay_wraps_low_level_gzip_corruption(tmp_path, monkeypatch):
    raw_root = _fixture_root(tmp_path)

    def corrupt(_payload):
        raise zlib.error("corrupt stream")

    monkeypatch.setattr(mod.gzip, "decompress", corrupt)

    with pytest.raises(mod.ReplayError, match="blob is missing or corrupt"):
        mod.build_replay_report(raw_root)


def test_replay_rejects_manifest_raw_uri_from_another_store(tmp_path):
    raw_root = _fixture_root(tmp_path)
    manifest = next((raw_root / "targets").rglob("*.json"))
    payload = json.loads(manifest.read_text(encoding="utf-8"))
    payload["raw_uri"] = f"https://evil.example/{payload['blob_key']}"
    manifest.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(mod.ReplayError, match="raw_uri"):
        mod.build_replay_report(raw_root)


def test_malformed_profile_url_is_ignored_without_leaking_urlparse_error():
    assert mod._profile_id("https://[") is None


def test_expected_summary_is_optional_but_exact(tmp_path):
    report = mod.build_replay_report(_fixture_root(tmp_path))
    mod.validate_expected_summary(
        report,
        {
            "catalog_id_count": 12,
            "decisions": report["summary"]["decisions"],
            "structural_gender": report["summary"]["structural_gender"],
        },
    )
    with pytest.raises(mod.ReplayError, match="expected summary"):
        mod.validate_expected_summary(report, {"catalog_id_count": 491})


def test_duplicate_404_observation_is_not_two_authoritative_misses(tmp_path):
    raw_root = _fixture_root(tmp_path)
    path = raw_root / "profile-observations.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    first_404 = next(
        item for item in payload["observations"] if item["competition_id"] == 9
    )
    payload["observations"] = [
        item for item in payload["observations"] if item["competition_id"] != 9
    ] + [first_404, dict(first_404)]
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = mod.build_replay_report(raw_root)
    decision = next(item for item in report["decisions"] if item["competition_id"] == 9)

    assert decision["decision"] == "pending_probe"
    assert decision["policy_rule"] == "probe_not_found"


def test_distinct_404_attempts_at_the_same_time_are_two_misses(tmp_path):
    raw_root = _fixture_root(tmp_path)
    path = raw_root / "profile-observations.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    misses = [
        item for item in payload["observations"] if item["competition_id"] == 9
    ]
    misses[1]["observed_at"] = misses[0]["observed_at"]
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = mod.build_replay_report(raw_root)
    decision = next(item for item in report["decisions"] if item["competition_id"] == 9)

    assert decision["decision"] == "excluded"
    assert decision["policy_rule"] == "exclude_dead_profile"


def test_observation_sidecar_must_match_the_replayed_catalog(tmp_path):
    raw_root = _fixture_root(tmp_path)
    path = raw_root / "profile-observations.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["catalog_content_hash"] = "0" * 64
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(mod.ReplayError, match="bound to the replayed catalog"):
        mod.build_replay_report(raw_root)


def test_observation_sidecar_invalid_utf8_is_a_clean_replay_error(tmp_path):
    raw_root = _fixture_root(tmp_path)
    (raw_root / "profile-observations.json").write_bytes(b"\xff")

    with pytest.raises(mod.ReplayError, match="invalid profile-observations.json"):
        mod.build_replay_report(raw_root)


def test_cli_writes_nothing_when_expected_summary_does_not_match(tmp_path):
    raw_root = _fixture_root(tmp_path)
    expected = tmp_path / "expected.json"
    output = tmp_path / "report.json"
    expected.write_text(json.dumps({"catalog_id_count": 491}), encoding="utf-8")

    assert (
        mod.main(
            [
                "--raw-root",
                str(raw_root),
                "--expected-summary",
                str(expected),
                "--output",
                str(output),
            ]
        )
        == 1
    )
    assert not output.exists()


def test_replay_does_not_accept_profile_payload_from_an_untrusted_host(tmp_path):
    raw_root = tmp_path / "raw"
    _store_catalog(raw_root, [{"id": 47, "name": "Premier League"}])
    _store_profile(raw_root, 47, "Premier League", host="evil.example")

    report = mod.build_replay_report(raw_root)

    assert report["summary"]["profile_payload_count"] == 0
    assert report["decisions"][0]["decision"] == "pending_probe"
    assert report["decisions"][0]["policy_rule"] == "probe_required"


def test_stored_source_validated_null_is_one_pending_miss_not_corrupt(tmp_path):
    raw_root = tmp_path / "raw"
    _store_catalog(raw_root, [{"id": 47, "name": "Placeholder"}])
    _store_payload(
        raw_root,
        "https://www.fotmob.com/api/data/leagues?id=47",
        None,
    )

    report = mod.build_replay_report(raw_root)

    assert report["summary"]["profile_payload_count"] == 1
    assert report["decisions"][0]["decision"] == "pending_probe"
    assert report["decisions"][0]["policy_rule"] == "probe_not_found"


def test_sidecar_cannot_invent_source_validated_null_without_raw_body(tmp_path):
    raw_root = tmp_path / "raw"
    catalog_hash = _store_catalog(
        raw_root, [{"id": 47, "name": "Placeholder"}]
    )
    catalog_url = "https://www.fotmob.com/api/data/allLeagues"
    profile_url = "https://www.fotmob.com/api/data/leagues?id=47"
    target_key = hashlib.sha256(profile_url.encode()).hexdigest()
    (raw_root / "profile-observations.json").write_text(
        json.dumps(
            {
                "schema_version": "fotmob-profile-observations-v1",
                "catalog_target_key": hashlib.sha256(
                    catalog_url.encode()
                ).hexdigest(),
                "catalog_content_hash": catalog_hash,
                "observations": [
                    {
                        "competition_id": 47,
                        "catalog_name": "Placeholder",
                        "outcome": "not_available",
                        "http_status": 200,
                        "json_null": True,
                        "profile_target_key": target_key,
                        "attempt_id": "invented-null-1",
                        "attempts": 1,
                        "source_validated": True,
                        "stale": False,
                        "observed_at": "2026-08-08T10:00:00+00:00",
                    },
                    {
                        "competition_id": 47,
                        "catalog_name": "Placeholder",
                        "outcome": "not_available",
                        "http_status": 304,
                        "json_null": True,
                        "profile_target_key": target_key,
                        "attempt_id": "invented-null-2",
                        "attempts": 1,
                        "source_validated": True,
                        "stale": False,
                        "observed_at": "2026-08-08T11:00:00+00:00",
                    },
                ],
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(mod.ReplayError, match="raw null manifest"):
        mod.build_replay_report(raw_root)


def test_null_manifest_with_later_304_proves_two_authoritative_misses(tmp_path):
    raw_root = tmp_path / "raw"
    _store_catalog(raw_root, [{"id": 47, "name": "Placeholder"}])
    _store_payload(
        raw_root,
        "https://www.fotmob.com/api/data/leagues?id=47",
        None,
        fetched_at="2026-08-08T10:00:00+00:00",
        validated_at="2026-08-08T11:00:00+00:00",
    )

    report = mod.build_replay_report(raw_root)

    assert report["decisions"][0]["decision"] == "excluded"
    assert report["decisions"][0]["policy_rule"] == "exclude_dead_profile"


def test_success_and_failure_at_the_same_time_are_rejected(tmp_path):
    raw_root = _fixture_root(tmp_path)
    path = raw_root / "profile-observations.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    profile_url = "https://www.fotmob.com/api/data/leagues?id=1"
    payload["observations"].append(
        {
            "competition_id": 1,
            "catalog_name": "Premier Cup",
            "outcome": "timeout",
            "profile_target_key": hashlib.sha256(profile_url.encode()).hexdigest(),
            "attempt_id": "probe-1-tie",
            "attempts": 1,
            "source_validated": False,
            "stale": False,
            "observed_at": "2026-08-08T10:00:00+00:00",
        }
    )
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(mod.ReplayError, match="ambiguous.*same timestamp"):
        mod.build_replay_report(raw_root)


@pytest.mark.parametrize("reverse", (False, True))
def test_conflicting_sidecar_events_at_one_time_are_order_independent(
    tmp_path, reverse
):
    raw_root = _fixture_root(tmp_path)
    path = raw_root / "profile-observations.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    profile_url = "https://www.fotmob.com/api/data/leagues?id=9"
    conflict = {
        "competition_id": 9,
        "catalog_name": "Missing Cup",
        "outcome": "timeout",
        "profile_target_key": hashlib.sha256(profile_url.encode()).hexdigest(),
        "attempt_id": "probe-9-tie",
        "attempts": 1,
        "source_validated": False,
        "stale": False,
        "observed_at": "2026-08-08T11:00:00+00:00",
    }
    if reverse:
        payload["observations"].insert(0, conflict)
    else:
        payload["observations"].append(conflict)
    path.write_text(json.dumps(payload), encoding="utf-8")

    with pytest.raises(mod.ReplayError, match="ambiguous.*same timestamp"):
        mod.build_replay_report(raw_root)


def test_latest_transient_probe_reopens_two_older_authoritative_misses(tmp_path):
    raw_root = _fixture_root(tmp_path)
    path = raw_root / "profile-observations.json"
    payload = json.loads(path.read_text(encoding="utf-8"))
    profile_url = "https://www.fotmob.com/api/data/leagues?id=9"
    payload["observations"].append(
        {
            "competition_id": 9,
            "catalog_name": "Missing Cup",
            "outcome": "timeout",
            "profile_target_key": hashlib.sha256(profile_url.encode()).hexdigest(),
            "attempt_id": "probe-9-3",
            "attempts": 1,
            "source_validated": False,
            "stale": False,
            "observed_at": "2026-08-08T12:00:00+00:00",
        }
    )
    path.write_text(json.dumps(payload), encoding="utf-8")

    report = mod.build_replay_report(raw_root)
    decision = next(item for item in report["decisions"] if item["competition_id"] == 9)

    assert decision["decision"] == "pending_probe"
    assert decision["policy_rule"] == "probe_retryable"


def test_later_transient_probe_replaces_an_older_success_with_pending(tmp_path):
    raw_root = tmp_path / "raw"
    catalog_hash = _store_catalog(
        raw_root, [{"id": 47, "name": "Premier League"}]
    )
    _store_profile(raw_root, 47, "Premier League")
    catalog_url = "https://www.fotmob.com/api/data/allLeagues"
    profile_url = "https://www.fotmob.com/api/data/leagues?id=47"
    (raw_root / "profile-observations.json").write_text(
        json.dumps(
            {
                "schema_version": "fotmob-profile-observations-v1",
                "catalog_target_key": hashlib.sha256(catalog_url.encode()).hexdigest(),
                "catalog_content_hash": catalog_hash,
                "observations": [
                    {
                        "competition_id": 47,
                        "catalog_name": "Premier League",
                        "outcome": "not_available",
                        "http_status": 503,
                        "json_null": True,
                        "profile_target_key": hashlib.sha256(
                            profile_url.encode()
                        ).hexdigest(),
                        "attempt_id": "probe-47-later",
                        "attempts": 1,
                        "source_validated": False,
                        "stale": True,
                        "observed_at": "2026-08-08T11:00:00+00:00",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = mod.build_replay_report(raw_root)

    assert report["decisions"][0]["decision"] == "pending_probe"
    assert report["decisions"][0]["policy_rule"] == "probe_retryable"
    assert report["decisions"][0]["profile_name"] == "Premier League"
    assert report["catalog_contract"]["included_ids"] == []


def test_newer_304_validation_of_old_profile_wins_over_earlier_failure(tmp_path):
    raw_root = tmp_path / "raw"
    catalog_hash = _store_catalog(
        raw_root, [{"id": 47, "name": "Premier League"}]
    )
    _store_profile(
        raw_root,
        47,
        "Premier League",
        fetched_at="2026-08-08T09:00:00+00:00",
        validated_at="2026-08-08T12:00:00+00:00",
    )
    catalog_url = "https://www.fotmob.com/api/data/allLeagues"
    profile_url = "https://www.fotmob.com/api/data/leagues?id=47"
    (raw_root / "profile-observations.json").write_text(
        json.dumps(
            {
                "schema_version": "fotmob-profile-observations-v1",
                "catalog_target_key": hashlib.sha256(catalog_url.encode()).hexdigest(),
                "catalog_content_hash": catalog_hash,
                "observations": [
                    {
                        "competition_id": 47,
                        "catalog_name": "Premier League",
                        "outcome": "timeout",
                        "profile_target_key": hashlib.sha256(
                            profile_url.encode()
                        ).hexdigest(),
                        "attempt_id": "probe-47-earlier",
                        "attempts": 1,
                        "source_validated": False,
                        "stale": True,
                        "observed_at": "2026-08-08T11:00:00+00:00",
                    }
                ],
            }
        ),
        encoding="utf-8",
    )

    report = mod.build_replay_report(raw_root)

    assert report["decisions"][0]["decision"] == "included"
    assert report["decisions"][0]["profile_fetched_at"] == (
        "2026-08-08T09:00:00+00:00"
    )
    assert report["decisions"][0]["observed_at"] == (
        "2026-08-08T12:00:00+00:00"
    )
