#!/usr/bin/env python3
"""Collect published club values inside the existing current/history lanes."""
from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
from typing import Any, Mapping
from urllib.parse import urlsplit, urlunsplit

from dags.utils.transfermarkt_club_values import write_club_values
from scrapers.transfermarkt.club_values import ClubValueCapture, parse_club_values, with_stichtag
from scrapers.transfermarkt.history_portion import enabled as history_portion_enabled
from scrapers.transfermarkt.models import FetchStatus, PROVIDER_GRANT_ENV_VAR
from scrapers.transfermarkt.scraper import TransfermarktScraper


class ClubValueRunError(RuntimeError):
    pass


def _scope(payload: Mapping[str, Any]) -> dict[str, Any]:
    required = {'scope_id', 'competition_id', 'edition_id', 'child_cycle_id',
                'resume_cycle_id', 'registry_snapshot_id', 'source_url'}
    if not required <= set(payload):
        raise ClubValueRunError('club-value scope payload is incomplete')
    return dict(payload)


def club_values_url(payload: Mapping[str, Any], stichtag: str | None = None) -> str:
    parsed = urlsplit(str(payload['source_url']))
    parts = parsed.path.strip('/').split('/')
    if (parsed.scheme != 'https' or parsed.netloc != 'www.transfermarkt.com'
            or parsed.username or parsed.password or parsed.query or parsed.fragment
            or len(parts) < 4 or parts[2] not in {'wettbewerb', 'pokalwettbewerb'}
            or parts[3] != str(payload['competition_id'])):
        raise ClubValueRunError('registry source URL cannot build club-value endpoint')
    path = '/' + '/'.join([parts[0], 'marktwerteverein', parts[2], parts[3]])
    base = urlunsplit(('https', parsed.netloc, path, '', ''))
    return with_stichtag(base, stichtag) if stichtag else base


def _preflight_from_env() -> dict[str, Any]:
    return {
        'paid_io_allowed': True,
        'write_mode': os.environ['TM_WRITE_MODE'],
        'revision': int(os.environ['TM_READER_REVISION']),
        'candidate_slot': int(os.environ['TM_CANDIDATE_SLOT']),
    }


def _fetch(scraper, url: str, payload: Mapping[str, Any]):
    outcome = scraper._fetch_endpoint_outcome(url, False, label='club_values')
    if outcome.status != FetchStatus.OK or not outcome.raw_capture_id or not isinstance(outcome.value, str):
        raise ClubValueRunError(f'club-value capture failed: {outcome.status.value}')
    body, raw = scraper._http_client._raw_store.load_capture(outcome.raw_capture_id)
    if (raw.url != url or raw.status_code != 200 or raw.scope_id != payload['scope_id']
            or raw.content_hash != outcome.raw_body_hash
            or hashlib.sha256(body).hexdigest() != outcome.raw_body_hash):
        raise ClubValueRunError('club-value raw lineage differs')
    return outcome, body.decode('utf-8')


def _extract(scraper, payload: Mapping[str, Any], url: str, html: str, outcome):
    capture = ClubValueCapture(
        competition_id=str(payload['competition_id']), source_url=url,
        raw_capture_id=outcome.raw_capture_id, source_body_hash=outcome.raw_body_hash,
        captured_at=outcome.raw_fetched_at, extracted_at=datetime.now(timezone.utc).isoformat(),
        scope_id=str(payload['scope_id']), run_id=str(payload['resume_cycle_id']),
        cycle_id=str(payload['child_cycle_id']), universe_version=str(payload['registry_snapshot_id']),
    )
    return parse_club_values(html, capture)


def run_club_values(scraper, payload: Mapping[str, Any], preflight: Mapping[str, Any], *,
                    mode: str, committed_at: datetime | None = None) -> dict[str, Any]:
    payload = _scope(payload)
    if mode not in {'current', 'history'}:
        raise ClubValueRunError('club-value mode must be current or history')
    url = club_values_url(payload)
    outcome, html = _fetch(scraper, url, payload)
    extraction = _extract(scraper, payload, url, html, outcome)
    history_url = None
    if mode == 'history':
        history_url = with_stichtag(url, extraction.available_dates[0])
        if history_url != url:
            outcome, html = _fetch(scraper, history_url, payload)
            history_extraction = _extract(scraper, payload, history_url, html, outcome)
            if history_extraction.club_ids != extraction.club_ids:
                raise ClubValueRunError('club-value earliest and current club universes differ')
            extraction = history_extraction
        if not extraction.full_available_history:
            raise ClubValueRunError('club-value earliest capture is not conclusive full history')
    result = write_club_values(scraper, extraction, preflight, committed_at=committed_at)
    return {
        'verified': True, 'mode': mode, 'scope_id': payload['scope_id'],
        'competition_id': payload['competition_id'], 'source_url': extraction.capture.source_url,
        'raw_capture_id': extraction.capture.raw_capture_id,
        'source_body_hash': extraction.capture.source_body_hash,
        'club_count': len(extraction.club_ids), 'point_count': len(extraction.rows),
        'status_counts': extraction.status_counts,
        'full_available_history': extraction.full_available_history,
        'receipts': result['receipts'], 'reconciliation': result['reconciliation'],
        'history_url': history_url,
    }


def _atomic_report(path: Path, report: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_suffix(path.suffix + '.tmp')
    temporary.write_text(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=True), encoding='utf-8')
    os.replace(temporary, path)


def main() -> int:
    from dags.scripts.run_transfermarkt_scraper import (
        _normalise_traffic,
        _prepare_cycle_budget,
        _record_cycle_traffic,
    )

    parser = argparse.ArgumentParser()
    parser.add_argument('--mode', choices=('current', 'history'), required=True)
    parser.add_argument('--report', required=True)
    args = parser.parse_args()
    payload = _scope(json.loads(os.environ['TM_SCOPE_PAYLOAD_JSON']))
    limit_bytes = int(os.environ['TM_PROVIDER_HARD_CAP_BYTES'])
    run_key = str(payload['child_cycle_id'])
    history_enabled = history_portion_enabled()
    if history_enabled:
        run_key += ':' + os.environ['TM_HISTORY_PORTION_ID']
    os.environ['TM_CYCLE_LEDGER_KEY'] = run_key
    budget = _prepare_cycle_budget(
        run_key, limit_bytes, entity='club_values', reserve_bytes=2 * 1024 * 1024,
    )
    scraper = None
    primary_error = None
    try:
        os.environ[PROVIDER_GRANT_ENV_VAR] = str(budget['reserved_bytes'])
        if history_enabled:
            os.environ['TM_HISTORY_GRANT_JSON'] = json.dumps({
                'ledger_path': str(budget['path']),
                'reservation_id': budget['reservation_id'],
                'cycle_ledger_key': run_key,
                'entity': 'club_values',
                'limit_bytes': budget['limit_bytes'],
                'reserved_bytes': budget['reserved_bytes'],
            })
        scraper = TransfermarktScraper(
            leagues=[str(payload['competition_id'])], seasons=[int(payload['edition_id'])],
            canonical_season=str(payload.get('canonical_season') or ''),
        )
        report = run_club_values(scraper, payload, _preflight_from_env(), mode=args.mode)
        _atomic_report(Path(args.report), report)
    except BaseException as exc:
        primary_error = exc
        raise
    finally:
        cleanup_errors = []
        traffic = None
        if scraper is not None:
            try:
                traffic = _normalise_traffic(scraper)
            except Exception as exc:
                cleanup_errors.append(exc)
            try:
                scraper.close()
            except Exception as exc:
                cleanup_errors.append(exc)
        try:
            if scraper is not None and (
                traffic is None or traffic.get('provider_metered_bytes') is None
            ):
                error = ClubValueRunError(
                    'club_values provider metered telemetry is unknown; '
                    'retaining the active reservation'
                )
                try:
                    _record_cycle_traffic(
                        run_key, limit_bytes, 'club_values:telemetry_unknown',
                        budget['remaining_after_reservation_bytes'],
                    )
                except Exception as exc:
                    error.add_note(f'Failed to reserve remaining provider budget: {exc!r}')
                raise error
            _record_cycle_traffic(
                run_key, limit_bytes, 'club_values',
                0 if scraper is None else traffic['provider_metered_bytes'],
                reservation_id=budget['reservation_id'],
            )
        except Exception as exc:
            cleanup_errors.append(exc)
        if cleanup_errors:
            if primary_error is not None:
                for error in cleanup_errors:
                    primary_error.add_note(f'Club-values reservation cleanup: {error!r}')
                    for note in getattr(error, '__notes__', ()):
                        primary_error.add_note(note)
            else:
                error = cleanup_errors[-1]
                for earlier in cleanup_errors[:-1]:
                    error.add_note(f'Club-values reservation cleanup: {earlier!r}')
                raise error
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
