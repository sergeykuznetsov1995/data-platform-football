from __future__ import annotations

from datetime import datetime, timezone
import hashlib
import json
import os
from typing import Any, Mapping

from scrapers.transfermarkt.attendance import (
    AttendanceExtraction,
    AttendancePage,
    canonical_timestamp,
    confirmed_empty_attendance,
    next_attendance_url,
    parse_attendance,
)
from scrapers.transfermarkt.models import FetchStatus


def capture_attendance_history(
    scraper,
    *,
    competition_record: Any,
    edition_id: str,
    expected_club_ids: tuple[str, ...],
    run_key: str,
    write_mode: str,
    reader_revision: int,
) -> tuple[AttendanceExtraction, Mapping[str, Any]]:
    from dags.utils.transfermarkt_attendance import write_attendance_observations

    competition_id = str(competition_record.competition_id)
    registry_snapshot_id = str(competition_record.registry_snapshot_id)
    scope_id = os.environ['TM_SCOPE_ID']
    start_url = (
        f'https://www.transfermarkt.com/{competition_record.slug}/besucherzahlen/'
        f'wettbewerb/{competition_id}/saison_id/{edition_id}'
    )
    extracted_at = datetime.now(timezone.utc).isoformat()
    if not expected_club_ids:
        proof = json.loads(os.environ.get('TM_EXPECTED_CLUB_PROOF_JSON', '{}'))
        if proof.get('listing_status') != 'authoritative_empty' \
                or proof.get('expected_team_ids') != [] \
                or not proof.get('listing_source_body_hash'):
            raise RuntimeError('empty attendance lacks an authoritative players checkpoint')
        observed_at = canonical_timestamp(proof.get('fetched_at'))
        result = confirmed_empty_attendance(
            completeness_proof='players-listing:' + proof['listing_source_body_hash'])
    else:
        pages = []
        url = start_url
        while url is not None:
            outcome = scraper._fetch_endpoint_outcome(
                url,
                label='attendance',
                context={
                    'scope': scope_id,
                    'cycle_id': run_key,
                    'competition_id': competition_id,
                    'edition_id': edition_id,
                    'traffic_reason': 'historical_attendance',
                },
            )
            if outcome.status != FetchStatus.OK or not outcome.raw_capture_id \
                    or not outcome.raw_fetched_at or not outcome.raw_body_hash:
                raise RuntimeError('attendance has no successful raw-first evidence')
            body, raw = scraper._http_client._raw_store.load_capture(outcome.raw_capture_id)
            if raw.url != url or raw.scope_id != scope_id \
                    or hashlib.sha256(body).hexdigest() != outcome.raw_body_hash:
                raise RuntimeError('attendance response lineage differs from durable raw receipt')
            pages.append(AttendancePage(
                raw.url, body, raw.capture_id, raw.content_hash, raw.fetched_at, raw.status_code,
            ))
            url = next_attendance_url(body, url)
        observed_at = pages[0].captured_at
        result = parse_attendance(
            pages,
            start_url=start_url,
            competition_id=competition_id,
            edition_id=edition_id,
            observed_at=observed_at,
        )
    extraction = AttendanceExtraction(
        result,
        scope_id,
        run_key,
        registry_snapshot_id,
        extracted_at,
        final_season=True,
        final_season_proof=f'history-backfill:{registry_snapshot_id}',
    )
    preflight = {
        'paid_io_allowed': True,
        'write_mode': write_mode,
        'revision': reader_revision,
    }
    receipt = write_attendance_observations(
        scraper,
        extraction,
        expected_club_ids,
        preflight,
        competition_id=competition_id,
        edition_id=edition_id,
        observed_at=observed_at,
    )
    expected = sorted(expected_club_ids, key=int)
    if not receipt.get('verified') or receipt.get('receipt_readback') is not True \
            or receipt.get('expected_club_ids') != expected \
            or receipt.get('observed_at') != observed_at \
            or receipt.get('final_season') is not True:
        raise RuntimeError('attendance writer receipt differs from historical edition proof')
    return extraction, receipt


def expected_attendance_clubs(value: str | None) -> tuple[str, ...]:
    if not value:
        raise RuntimeError('historical attendance requires the players participant checkpoint')
    loaded = json.loads(value)
    if not isinstance(loaded, list) or any(not isinstance(item, str) or not item for item in loaded):
        raise RuntimeError('historical attendance participant checkpoint is invalid')
    return tuple(loaded)
