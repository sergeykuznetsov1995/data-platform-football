import json

import pytest

from dags.scripts.dispose_sofascore_moved_seasons import (
    DispositionError,
    apply_disposition,
    build_disposition,
)
from scrapers.sofascore.manifest import EndpointManifest, ManifestKey, ManifestStatus


def _record(scope, endpoint, status, target_id="1"):
    return EndpointManifest(
        key=ManifestKey(*scope, "match", target_id, endpoint, "final"),
        status=status,
        run_id="old-run",
        task_id="capture",
        attempts=1,
        row_count=1,
        http_status=200,
        raw_content_hash="hash",
        raw_blob_key="blob",
    )


def _payload(season_id, tournament_id="11058", event_id=1):
    return json.dumps({
        "event": {
            "id": event_id,
            "season": {"id": season_id},
            "tournament": {"uniqueTournament": {"id": tournament_id}},
        }
    }).encode()


def test_moved_scope_disposition_is_evidence_based_and_applied():
    old_scope = ("11058", "74464")
    new_scope = ("11058", "84686")
    records_by_scope = {
        old_scope: [
            _record(old_scope, "event", ManifestStatus.RETRYABLE_FAILURE),
            _record(old_scope, "incidents", ManifestStatus.RETRYABLE_FAILURE),
        ],
        new_scope: [_record(new_scope, "event", ManifestStatus.SUCCESS)],
    }
    store = type("Store", (), {"upsert": lambda self, record: None})()
    calls = []
    store.upsert = lambda record: calls.append(record)

    plan = build_disposition(
        records_by_scope,
        lambda key: _payload(84686),
        run_id="moved-test",
        scopes={old_scope: new_scope},
    )
    apply_disposition(store, plan)

    assert len(calls) == 2
    assert {record.status for record in calls} == {ManifestStatus.SCHEMA_ERROR}
    assert {record.error_type for record in calls} == {"MovedSourceSeason"}
    assert {record.run_id for record in calls} == {"moved-test"}
    assert {record.attempts for record in calls} == {2}


def test_disposition_rejects_wrong_raw_season_before_any_write():
    records_by_scope = {
        ("11058", "74464"): [
            _record(("11058", "74464"), "event", ManifestStatus.RETRYABLE_FAILURE)
        ],
        ("11058", "84686"): [],
    }
    records_by_scope[("11058", "84686")] = [
        _record(("11058", "84686"), "event", ManifestStatus.SUCCESS)
    ]

    with pytest.raises(DispositionError, match="unexpected payload season"):
        build_disposition(
            records_by_scope,
            lambda key: _payload(99999),
            run_id="x",
            scopes={("11058", "74464"): ("11058", "84686")},
        )


def test_disposition_requires_settled_correct_scope():
    records_by_scope = {
        ("11058", "74464"): [
            _record(("11058", "74464"), "event", ManifestStatus.RETRYABLE_FAILURE)
        ],
        ("11058", "84686"): [],
    }
    records_by_scope[("11058", "84686")] = [
        _record(("11058", "84686"), "event", ManifestStatus.RETRYABLE_FAILURE)
    ]

    with pytest.raises(DispositionError, match="no success evidence"):
        build_disposition(
            records_by_scope,
            lambda key: _payload(84686),
            run_id="x",
            scopes={("11058", "74464"): ("11058", "84686")},
        )


def test_disposition_requires_event_evidence():
    records_by_scope = {
        ("11058", "74464"): [
            _record(("11058", "74464"), "incidents", ManifestStatus.RETRYABLE_FAILURE)
        ],
        ("11058", "84686"): [],
    }
    records_by_scope[("11058", "84686")] = [
        _record(("11058", "84686"), "event", ManifestStatus.SUCCESS)
    ]

    with pytest.raises(DispositionError, match="no event evidence"):
        build_disposition(
            records_by_scope,
            lambda key: _payload(84686),
            run_id="x",
            scopes={("11058", "74464"): ("11058", "84686")},
        )



def test_disposition_rejects_record_without_verified_event():
    records_by_scope = {
        ("11058", "74464"): [
            _record(("11058", "74464"), "event", ManifestStatus.RETRYABLE_FAILURE),
            _record(
                ("11058", "74464"), "incidents", ManifestStatus.RETRYABLE_FAILURE,
                target_id="other-match",
            ),
        ],
        ("11058", "84686"): [],
    }
    records_by_scope[("11058", "84686")] = [
        _record(("11058", "84686"), "event", ManifestStatus.SUCCESS)
    ]

    with pytest.raises(DispositionError, match="without a verified event"):
        build_disposition(
            records_by_scope,
            lambda key: _payload(84686),
            run_id="x",
            scopes={("11058", "74464"): ("11058", "84686")},
        )


def test_disposition_empty_scope_is_idempotent():
    plan = build_disposition(
        {
            ("11058", "74464"): [],
            ("11058", "84686"): [],
        },
        lambda key: _payload(84686),
        run_id="x",
        scopes={("11058", "74464"): ("11058", "84686")},
    )

    assert plan == ((("11058", "74464"), ("11058", "84686"), ()),)



def test_disposition_rejects_payload_event_id_mismatch_without_writes():
    records_by_scope = {
        ("11058", "74464"): [
            _record(("11058", "74464"), "event", ManifestStatus.RETRYABLE_FAILURE)
        ],
        ("11058", "84686"): [],
    }
    records_by_scope[("11058", "84686")] = [
        _record(("11058", "84686"), "event", ManifestStatus.SUCCESS)
    ]
    calls = []
    store = type("Store", (), {"upsert": lambda self, record: None})()
    store.upsert = lambda record: calls.append(record)

    with pytest.raises(DispositionError, match="payload event id mismatch"):
        build_disposition(
            records_by_scope,
            lambda key: _payload(84686, event_id=999),
            run_id="x",
            scopes={("11058", "74464"): ("11058", "84686")},
        )
    apply_disposition(store, ())
    assert calls == []
