#!/usr/bin/env python3
"""Run one Transfermarkt Bronze entity in an isolated process.

The CLI keeps the four legacy entity names and exit-code contract used by the
Airflow DAG.  A native-v2 scrape is dual-written to the new, correctly-grained
tables and the legacy tables from the *same* response bundle, so migration does
not double residential-proxy traffic.

Exit codes: 0 success/no-op/dry-run, 1 hard failure, 2 genuine empty fallback,
3 replace-partition completeness guard refusal.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
import re
import sys
import uuid
import warnings
from dataclasses import dataclass, replace
from datetime import date, datetime, timedelta, timezone
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

from dags.utils.transfermarkt_dq_contracts import (
    ScopeDQError,
    input_from_capture,
    validate_scope_capture,
)

warnings.filterwarnings('ignore', category=DeprecationWarning)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(name)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
logger = logging.getLogger(__name__)

_URL_CREDENTIALS_RE = re.compile(
    r'(?P<scheme>(?:https?|socks[45])://)(?P<credentials>[^/@\s]+)@',
    re.IGNORECASE,
)


def _redact_sensitive(value: Any) -> str:
    """Defense-in-depth: never persist or log proxy URL credentials."""

    return _URL_CREDENTIALS_RE.sub(r'\g<scheme>****:****@', str(value))


def _redact_value(value: Any):
    if isinstance(value, Mapping):
        return {
            _redact_sensitive(key) if isinstance(key, str) else key:
            _redact_value(item)
            for key, item in value.items()
        }
    if isinstance(value, list):
        return [_redact_value(item) for item in value]
    if isinstance(value, tuple):
        return tuple(_redact_value(item) for item in value)
    if isinstance(value, str):
        return _redact_sensitive(value)
    return value


class _ArgparseError(Exception):
    """CLI errors must map to hard-failure exit 1, never soft exit 2."""


class _EmptyRosterError(RuntimeError):
    """Anchor Bronze roster has no keys; dependent entity is a soft fallback."""


class _StrictArgumentParser(argparse.ArgumentParser):
    def error(self, message):
        self.print_usage(sys.stderr)
        raise _ArgparseError(message)


class _EntityRunComplete(Exception):
    """Internal non-error control flow that still executes final traffic gates."""

    def __init__(self, exit_code: int):
        super().__init__(exit_code)
        self.exit_code = int(exit_code)


ENTITY_PLAYERS = 'players'
ENTITY_MV_HISTORY = 'market_value_history'
ENTITY_TRANSFERS = 'transfers'
ENTITY_COACHES = 'coaches'
VALID_ENTITIES = {
    ENTITY_PLAYERS, ENTITY_MV_HISTORY, ENTITY_TRANSFERS, ENTITY_COACHES,
}

REPLACE_GUARD_MARKER = 'TM_REPLACE_GUARD'
_MIN_REPLACE_RATIO = 0.9
_WINDOW_STRIDE_DAYS = 7
MAX_ROSTER_WINDOW = 100
FETCH_STATE_TABLE = 'iceberg.ops.transfermarkt_fetch_state'
PARSER_VERSION = os.environ.get('TM_PARSER_VERSION', 'v2')
SCHEMA_VERSION = os.environ.get('TM_SCHEMA_VERSION', '2')
REFRESH_MODES = {'auto', 'current', 'historical', 'force'}
WRITE_MODES = {'dual', 'native-only', 'legacy-only'}
DUAL_WRITE_MANIFEST_TABLE = 'iceberg.ops.transfermarkt_dual_write_manifest_v2'
NATIVE_WRITE_MANIFEST_TABLE = (
    'iceberg.ops.transfermarkt_native_write_manifest_v2'
)
PENDING_CHECKPOINT_TTL_DAYS = int(
    os.environ.get('TM_PENDING_CHECKPOINT_TTL_DAYS', '35')
)
MIB = 1024 * 1024
PRODUCTION_CYCLE_BUDGET_BYTES = 15 * MIB
# 'requests' counts attempts, not pages; keep in sync with the parent cycle's
# DEFAULT_ENTITY_LIMITS.
PRODUCTION_ENTITY_BUDGETS = {
    ENTITY_PLAYERS: {'decoded_mb': 16.0, 'requests': 150},
    ENTITY_MV_HISTORY: {'decoded_mb': 4.0, 'requests': 200},
    ENTITY_TRANSFERS: {'decoded_mb': 8.0, 'requests': 200},
    ENTITY_COACHES: {'decoded_mb': 14.0, 'requests': 160},
}


@dataclass(frozen=True)
class OutputSpec:
    key: str
    table_name: str
    partition_cols: Tuple[str, ...] = ()
    replace_keys: Tuple[str, ...] = ()
    guard_key: Optional[str] = None
    is_legacy: bool = False


@dataclass(frozen=True)
class EntitySpec:
    name: str
    id_column: str
    count_field: str
    state_endpoint: Optional[str]
    native_reader: str
    legacy_reader: str
    outputs: Tuple[OutputSpec, ...]


ENTITY_SPECS: Dict[str, EntitySpec] = {
    ENTITY_PLAYERS: EntitySpec(
        name=ENTITY_PLAYERS,
        id_column='player_id',
        count_field='players_with_rows',
        state_endpoint=None,
        native_reader='read_squad_data',
        legacy_reader='read_players',
        outputs=(
            OutputSpec(
                'memberships', 'transfermarkt_squad_memberships',
                ('competition_id', 'edition_id'),
                ('competition_id', 'edition_id'), 'player_id',
            ),
            OutputSpec(
                'attribute_observations',
                'transfermarkt_player_attribute_observations',
                ('competition_id', 'edition_id'), (), None,
            ),
            OutputSpec(
                'contract_observations',
                'transfermarkt_player_contract_observations',
                ('competition_id', 'edition_id'),
                ('competition_id', 'edition_id'), 'player_id',
            ),
            OutputSpec(
                'legacy_players', 'transfermarkt_players',
                ('league', 'season'), ('league', 'season'), 'player_id', True,
            ),
        ),
    ),
    ENTITY_MV_HISTORY: EntitySpec(
        name=ENTITY_MV_HISTORY,
        id_column='player_id',
        count_field='players_with_rows',
        state_endpoint='market_value_points',
        native_reader='read_market_value_points',
        legacy_reader='read_market_value_history',
        outputs=(
            OutputSpec(
                'market_value_points', 'transfermarkt_market_value_points',
                (), ('player_id',), 'player_id',
            ),
            OutputSpec(
                'legacy_market_value_history',
                'transfermarkt_market_value_history',
                ('league', 'season'), ('league', 'season', 'player_id'),
                'player_id', True,
            ),
        ),
    ),
    ENTITY_TRANSFERS: EntitySpec(
        name=ENTITY_TRANSFERS,
        id_column='player_id',
        count_field='players_with_rows',
        state_endpoint='transfer_events',
        native_reader='read_transfer_events',
        legacy_reader='read_transfers',
        outputs=(
            OutputSpec(
                'transfer_events', 'transfermarkt_transfer_events',
                (), ('player_id',), 'player_id',
            ),
            OutputSpec(
                'legacy_transfers', 'transfermarkt_transfers',
                ('league', 'season'), ('league', 'season', 'player_id'),
                'player_id', True,
            ),
        ),
    ),
    ENTITY_COACHES: EntitySpec(
        name=ENTITY_COACHES,
        id_column='coach_id',
        count_field='coaches_with_rows',
        state_endpoint=None,
        native_reader='read_coach_data',
        legacy_reader='read_coaches',
        outputs=(
            OutputSpec(
                'profiles', 'transfermarkt_coach_profiles',
                (), ('coach_id',), 'coach_id',
            ),
            OutputSpec(
                'stints', 'transfermarkt_coach_stints',
                (), ('club_id',), None,
            ),
            OutputSpec(
                'legacy_coaches', 'transfermarkt_coaches',
                ('league', 'season'),
                ('league', 'season', 'current_club_id'),
                'coach_id', True,
            ),
        ),
    ),
}

COACH_HISTORY_CHECKPOINT_SPEC = replace(
    ENTITY_SPECS[ENTITY_COACHES],
    id_column='club_id',
    state_endpoint='coach_history',
)


def _normalise_write_mode(
    write_mode: Optional[str],
    *,
    native_dual_write: bool = True,
) -> str:
    """Resolve the explicit writer lifecycle without ambiguous booleans.

    ``native_dual_write`` remains as a rollback-compatible API for callers
    created before cleanup support.  New production orchestration passes the
    state-derived ``write_mode`` explicitly.
    """
    if write_mode is None:
        return 'dual' if native_dual_write else 'legacy-only'
    value = str(write_mode).strip().lower()
    if value not in WRITE_MODES:
        raise ValueError(
            f'write_mode must be one of {sorted(WRITE_MODES)}, got {write_mode!r}'
        )
    return value


def _spec_for_write_mode(spec: EntitySpec, write_mode: str) -> EntitySpec:
    """Return the exact physical outputs authorized for this lifecycle."""
    mode = _normalise_write_mode(write_mode)
    outputs = tuple(
        output for output in spec.outputs
        if (
            mode == 'dual'
            or (mode == 'native-only' and not output.is_legacy)
            or (mode == 'legacy-only' and output.is_legacy)
        )
    )
    if not outputs:
        raise RuntimeError(f'no {mode} output contract for {spec.name}')
    return replace(spec, outputs=outputs)


def _writer_mode_state_error(
    state: Any,
    write_mode: str,
    expected_revision: int,
) -> Optional[str]:
    """Return why persisted state does not authorize this physical writer."""
    mode = _normalise_write_mode(write_mode)
    revision = int(expected_revision)
    if revision < 0:
        return 'expected reader revision must be non-negative'
    exists = bool(getattr(state, 'exists', False))
    actual_revision = int(getattr(state, 'revision', 0) or 0)
    if actual_revision != revision:
        return (
            f'reader revision drift: expected={revision}, '
            f'actual={actual_revision}'
        )
    if not exists:
        return f'{mode} requires persisted reader state'
    active_version = str(getattr(state, 'active_version', ''))
    writers_disabled = getattr(state, 'legacy_writers_disabled_at', None)
    cleanup_completed = getattr(state, 'cleanup_completed_at', None)
    if mode == 'dual':
        if writers_disabled is not None or cleanup_completed is not None:
            return 'dual writes are forbidden after legacy writers are disabled'
        if active_version not in {'legacy', 'v2'}:
            return f'dual writes reject malformed route {active_version!r}'
        return None
    if mode == 'native-only':
        if active_version != 'v2' or writers_disabled is None:
            return (
                'native-only requires active v2 and persisted '
                'legacy_writers_disabled_at'
            )
        return None
    if active_version != 'legacy' or writers_disabled is not None:
        return (
            'legacy-only requires an audited active legacy route before '
            'writer disablement'
        )
    return None


def _authorize_write_mode(write_mode: str, expected_revision: int) -> Dict[str, Any]:
    """Read ops state and authorize the requested writer before any proxy I/O."""
    try:
        from utils import transfermarkt_native_v2 as control
    except ImportError:  # standalone package execution
        from dags.utils import transfermarkt_native_v2 as control

    revision = int(expected_revision)
    conn = control.connect()
    cur = conn.cursor()
    try:
        state = control.read_reader_state(
            cur, allow_missing=(revision == 0),
        )
    finally:
        try:
            cur.close()
        finally:
            conn.close()
    error = _writer_mode_state_error(state, write_mode, revision)
    if error:
        raise RuntimeError(
            'persisted Transfermarkt writer mode rejected before paid I/O: '
            + error
        )
    return {
        'write_mode': _normalise_write_mode(write_mode),
        'expected_revision': revision,
        'state_exists': bool(getattr(state, 'exists', False)),
        'active_version': getattr(state, 'active_version', 'legacy'),
        'active_slot': getattr(state, 'active_slot', None),
        'legacy_writers_disabled': (
            getattr(state, 'legacy_writers_disabled_at', None) is not None
        ),
        'cleanup_completed': (
            getattr(state, 'cleanup_completed_at', None) is not None
        ),
    }

_DERIVED_STATE_SOURCES = {
    'market_value_points': (
        'iceberg.bronze.transfermarkt_market_value_points', 'player_id',
    ),
    'transfer_events': (
        'iceberg.bronze.transfermarkt_transfer_events', 'player_id',
    ),
    'coach_history': (
        'iceberg.bronze.transfermarkt_coach_stints', 'club_id',
    ),
}


def _window_offset(as_of_date: Optional[str]) -> int:
    """Stable weekly window index; reruns for the same logical date agree."""
    d = date.fromisoformat(as_of_date) if as_of_date else date.today()
    return d.toordinal() // _WINDOW_STRIDE_DAYS


def _default_run_key(
    league: str, season: int, as_of_date: Optional[str], explicit: Optional[str],
) -> str:
    return (
        explicit
        or os.environ.get('AIRFLOW_CTX_DAG_RUN_ID')
        or f"manual:{league}:{season}:{as_of_date or date.today().isoformat()}"
    )


def _resolved_refresh_mode(mode: str, season: int) -> str:
    if mode != 'auto':
        return mode
    # Match utils.config.get_current_season without importing Airflow/DAG code.
    today = date.today()
    current_season = today.year if today.month >= 8 else today.year - 1
    return 'current' if int(season) == current_season else 'historical'


def _competition_record(value: str):
    """Resolve through the same central registry used by the scraper."""

    from scrapers.transfermarkt.registry import (
        CompetitionRecord,
        resolve_competition,
    )

    raw = os.environ.get('TM_COMPETITION_RECORDS_JSON', '').strip()
    records = None
    if raw:
        decoded = json.loads(raw)
        if not isinstance(decoded, list):
            raise ValueError('TM_COMPETITION_RECORDS_JSON must be a JSON list')
        records = tuple(CompetitionRecord.from_mapping(item) for item in decoded)
    return resolve_competition(value, records=records)


def _canonical_scope_season(competition: str, edition_id: int | str) -> str:
    from scrapers.transfermarkt.registry import canonical_season

    record = _competition_record(competition)
    if not record.crawl_eligible:
        raise ValueError(
            f'{record.competition_id}: classification blocks crawl: '
            f'{record.crawl_block_reason}'
        )
    # The source offsets some calendar leagues' saison_id from the season it
    # names, so the registered season — not the edition id — is the truth. The
    # edition id is only a fallback for a caller that states no season.
    registered = str(os.environ.get('TM_CANONICAL_SEASON') or '').strip()
    if registered:
        return registered
    return canonical_season(edition_id, record.season_format)


def _scope_season(competition: str, edition_id: int | str) -> Dict[str, Any]:
    """The season a dated projection must use, and the format that dates it."""

    from scrapers.transfermarkt.registry import season_window_year

    record = _competition_record(competition)
    canonical = _canonical_scope_season(competition, edition_id)
    return {
        'canonical_season': canonical,
        'season_format': record.season_format,
        'season_year': season_window_year(
            edition_id, record.season_format, canonical,
        ),
    }


def _compatibility_league(competition: str) -> str:
    record = _competition_record(competition)
    return record.canonical_competition_id or f'TM-{record.competition_id}'


def _proxy_traffic_module():
    """Work both under ``PYTHONPATH=dags`` and as ``python -m dags...``."""
    try:
        from utils import proxy_traffic
    except ImportError:
        from dags.utils import proxy_traffic
    return proxy_traffic


def _normalise_traffic(scraper) -> Dict[str, Any]:
    """Capture counters after every outcome, preserving historical aliases."""
    try:
        raw = scraper.get_traffic_stats()
        traffic = dict(raw) if isinstance(raw, Mapping) else {}
    except Exception as exc:  # noqa: BLE001 - result must still be written
        safe_error = _redact_sensitive(exc)
        logger.warning('get_traffic_stats failed: %s', safe_error)
        return {'telemetry_available': False, 'telemetry_error': safe_error}

    try:
        stats = scraper.get_stats()
        stats = stats if isinstance(stats, Mapping) else {}
    except Exception:  # noqa: BLE001
        stats = {}

    byte_keys = (
        'decoded_response_body_bytes', 'proxy_response_bytes', 'fs_response_bytes',
    )
    mb_keys = (
        'decoded_response_body_mb', 'proxy_response_mb', 'fs_response_mb',
    )
    decoded_bytes = next(
        (traffic[k] for k in byte_keys if traffic.get(k) is not None), None,
    )
    decoded_mb = next(
        (traffic[k] for k in mb_keys if traffic.get(k) is not None), None,
    )
    # Rounded MiB telemetry is display-only and must never be expanded back
    # into fabricated "raw" bytes.  The shared paid-traffic guard is exact;
    # legacy MB-only telemetry therefore fails closed.
    try:
        decoded_bytes = int(decoded_bytes) if decoded_bytes is not None else None
    except (TypeError, ValueError, OverflowError):
        decoded_bytes = None
    if decoded_bytes is not None and decoded_bytes < 0:
        decoded_bytes = None
    available = decoded_bytes is not None
    traffic['telemetry_available'] = available
    if available:
        decoded_mb = round(decoded_bytes / 1024 / 1024, 4)
        traffic['decoded_response_body_bytes'] = decoded_bytes
        traffic['decoded_response_body_mb'] = decoded_mb
        # Old consumers read this tls_requests alias; keep it through cutover.
        traffic.setdefault('proxy_response_mb', decoded_mb)
    else:
        for key in byte_keys:
            traffic.pop(key, None)
        traffic['telemetry_error'] = (
            'raw decoded_response_body_bytes unavailable; rounded MiB '
            'telemetry is not authoritative'
        )

    requests = int(
        traffic.get('network_fetches', traffic.get('requests', stats.get('requests', 0)))
        or 0
    )
    retries = int(traffic.get('retries', stats.get('retries', 0)) or 0)
    failures = int(
        traffic.get(
            'failures',
            traffic.get('failed_attempts', stats.get('failures', 0)),
        ) or 0
    )
    traffic.setdefault('requests', requests)  # historical alias
    traffic.setdefault('network_fetches', requests)
    traffic.setdefault('retries', retries)
    traffic.setdefault('failures', failures)
    traffic.setdefault('failed_attempts', failures)
    traffic.setdefault('cache_hits', int(traffic.get('cache_hits', 0) or 0))
    estimated_wire = traffic.get(
        'estimated_wire_response_mb', traffic.get('wire_mb'),
    )
    traffic.setdefault('estimated_wire_response_mb', estimated_wire)
    traffic.setdefault('wire_mb', estimated_wire)
    return traffic


def _write_results(
    path: str,
    payload: Dict[str, Any],
    *,
    persist_traffic: bool = True,
) -> None:
    """Write/print/persist exactly once for one entity subprocess."""
    safe_payload = _redact_value(payload)
    try:
        with open(path, 'w') as fh:
            json.dump(safe_payload, fh, default=str)
    except Exception as exc:  # noqa: BLE001
        logger.warning(
            'Could not write results to %s: %s', path, _redact_sensitive(exc),
        )
    try:
        print(json.dumps(safe_payload, default=str))
    except Exception:  # noqa: BLE001
        pass

    if not persist_traffic:
        logger.info('Skipping proxy-traffic persistence for read-only run')
        return

    traffic = safe_payload.get('traffic')
    if not isinstance(traffic, Mapping) or not traffic.get('telemetry_available'):
        logger.error(
            'PROXY_TRAFFIC telemetry unavailable for %s',
            safe_payload.get('entity'),
        )
        return
    try:
        proxy_traffic = _proxy_traffic_module()
        summary = proxy_traffic.summarize_result_traffic(
            'transfermarkt', dict(traffic), entity=safe_payload.get('entity'),
            run_key=safe_payload.get('run_key'),
        )
        proxy_traffic.log_traffic_summary(summary)
        proxy_traffic.record_traffic_run(
            summary,
            dag_run_id=os.environ.get('AIRFLOW_CTX_DAG_RUN_ID', ''),
        )
    except Exception as exc:  # noqa: BLE001 - ingest result is authoritative
        logger.warning(
            'proxy-traffic persistence failed: %s', _redact_sensitive(exc),
        )


def _classify_fallback(scraper) -> str:
    try:
        outcomes = scraper.get_fetch_outcomes()
        statuses = {
            item.get('status')
            for endpoint in (outcomes or {}).values()
            for item in (endpoint or {}).values()
            if isinstance(item, Mapping)
        }
        for hard_status in ('schema_error', 'blocked', 'retry_exhausted'):
            if hard_status in statuses:
                return hard_status
    except Exception:  # noqa: BLE001 - legacy scraper compatibility
        pass
    last_err = getattr(scraper, '_last_endpoint_error', None)
    if not last_err:
        return 'empty_payload'
    if isinstance(last_err, Mapping):
        outcome = last_err.get('outcome') or last_err.get('kind')
        if outcome in {'blocked', 'retry_exhausted', 'schema_error'}:
            return str(outcome)
        status = last_err.get('status')
    else:
        status = getattr(last_err, 'status', None)
    if status == 403:
        return 'http_403'
    if status == 429:
        return 'http_429'
    if status is None:
        return 'transport_error'
    return f'http_{status}'


def _fallback_exit_code(reason: str) -> int:
    hard = (
        reason.startswith('http_')
        or reason in {
            'transport_error', 'blocked', 'retry_exhausted', 'schema_error',
            'proxy_unavailable', 'traffic_budget_exceeded',
        }
    )
    return 1 if hard else 2


def _execute_cursor(conn, sql: str, params: Sequence[Any] = (), fetch=False):
    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params))
        # Trino executes lazily; closing an unconsumed DDL/DML cursor cancels it
        # as USER_CANCELED. Always drain the result stream before close.
        rows = cur.fetchall()
        return rows if fetch else None
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001
            pass


def _ensure_fetch_state(conn) -> None:
    _execute_cursor(conn, 'CREATE SCHEMA IF NOT EXISTS iceberg.ops')
    _execute_cursor(
        conn,
        f"CREATE TABLE IF NOT EXISTS {FETCH_STATE_TABLE} ("
        "endpoint varchar, source_id varchar, parser_version varchar, "
        "schema_version varchar, "
        "status varchar, run_key varchar, first_attempt_at timestamp(6), "
        "last_attempt_at timestamp(6), last_success_at timestamp(6), "
        "row_count bigint, payload_hash varchar, error varchar"
        ") WITH (format = 'PARQUET')",
    )
    _execute_cursor(
        conn,
        f"ALTER TABLE {FETCH_STATE_TABLE} "
        "ADD COLUMN IF NOT EXISTS schema_version varchar",
    )


def _load_fetch_state(
    scraper, endpoint: str, *, strict: bool = False,
) -> Dict[str, Dict[str, Any]]:
    """Read checkpoint state.

    A genuinely absent table is a valid first-run cache miss. Other errors are
    fatal in historical mode: silently re-fetching a whole paid window because
    Trino is unhealthy defeats the cache's purpose.
    """
    try:
        conn = scraper._bronze_connection()
        rows = _execute_cursor(
            conn,
            f"SELECT source_id, status, run_key, last_success_at "
            f"FROM {FETCH_STATE_TABLE} "
            "WHERE endpoint = ? AND parser_version = ? AND schema_version = ?",
            (endpoint, PARSER_VERSION, SCHEMA_VERSION),
            fetch=True,
        ) or []
        return {
            str(row[0]): {
                'status': row[1], 'run_key': row[2], 'last_success_at': row[3],
            }
            for row in rows if row and row[0] is not None
        }
    except Exception as exc:  # noqa: BLE001
        message = str(exc).lower()
        missing = any(token in message for token in (
            'table_not_found', 'table not found', 'does not exist',
        ))
        if missing:
            logger.info('fetch state table absent; treating as cold cache')
            return {}
        if strict:
            raise RuntimeError(
                'fetch state unavailable; refusing historical proxy refetch: '
                f'{_redact_sensitive(exc)}'
            ) from exc
        logger.warning(
            'fetch state unavailable; using bounded current window: %s',
            _redact_sensitive(exc),
        )
        return {}


def _load_data_derived_state(
    scraper,
    endpoint: str,
    league: str,
    season: int,
    roster: Sequence[str],
) -> Dict[str, Dict[str, Any]]:
    """Derive cache only from parity-proven commits or bootstrap parity.

    Native row presence alone is unsafe: a native table may have committed
    before a legacy write/manifest failed. Runtime rows therefore require a
    successful pair manifest. Bootstrap rows are accepted only when native and
    requested legacy compatibility-key sets are exactly equal per source ID.
    """
    source = _DERIVED_STATE_SOURCES.get(endpoint)
    if source is None:
        return {}
    table, key_column = source
    entity = {
        'market_value_points': 'market_value_points',
        'transfer_events': 'transfer_events',
        'coach_history': 'coach_stints',
    }[endpoint]
    roster = [str(value) for value in dict.fromkeys(roster)]
    if not roster:
        return {}

    placeholders = ', '.join('?' for _ in roster)
    try:
        conn = scraper._bronze_connection()
        rows = _execute_cursor(
            conn,
            f"SELECT CAST(n.{key_column} AS varchar), count(*), max(n._ingested_at) "
            f"FROM {table} n "
            f"WHERE CAST(n.{key_column} AS varchar) IN ({placeholders}) "
            f"AND EXISTS (SELECT 1 FROM {DUAL_WRITE_MANIFEST_TABLE} m "
            "WHERE m.entity = ? AND m.status = 'success' "
            "AND m.legacy_batch_id = m.native_batch_id "
            "AND m.legacy_rows = m.native_rows "
            "AND m.legacy_hash = m.native_hash "
            "AND m.native_batch_id = n._batch_id) "
            f"GROUP BY n.{key_column}",
            tuple(roster) + (entity,),
            fetch=True,
        ) or []
        derived = {
            str(row[0]): {
                'status': 'success',
                'run_key': 'manifest-derived',
                'last_success_at': row[2],
                'row_count': int(row[1] or 0),
                'derived': 'manifest',
            }
            for row in rows if row and row[0] is not None
        }
    except Exception as exc:  # noqa: BLE001
        message = str(exc).lower()
        if any(token in message for token in (
            'table_not_found', 'table not found', 'does not exist',
        )):
            derived = {}
        else:
            raise RuntimeError(
                f'native data-derived cache unavailable for {endpoint}: {exc}'
            ) from exc

    if endpoint not in {'market_value_points', 'transfer_events'}:
        return derived

    # Bootstrap migration predates the runtime manifest. Prove exact per-player
    # compatibility against any complete legacy projection.  A historical
    # partition that does not exist yet can then be rebuilt locally; proof is
    # never inferred from native row presence alone.
    columns = (
        ('mv_date',)
        if endpoint == 'market_value_points'
        else (
            'transfer_date', 'from_club_id', 'from_club_name',
            'to_club_id', 'to_club_name', 'fee_text', 'is_upcoming',
        )
    )
    legacy_table = (
        'iceberg.bronze.transfermarkt_market_value_history'
        if endpoint == 'market_value_points'
        else 'iceberg.bronze.transfermarkt_transfers'
    )
    select_columns = ', '.join(columns)
    season_short = _canonical_scope_season(league, season)

    def normalise(value: Any) -> str:
        if value is None or str(value) in {'nan', 'NaT', '<NA>'}:
            return '__NULL__'
        return str(value)

    try:
        conn = scraper._bronze_connection()
        native_rows = _execute_cursor(
            conn,
            f"SELECT CAST(player_id AS varchar), {select_columns}, "
            "_ingested_at, _batch_id "
            f"FROM {table} WHERE CAST(player_id AS varchar) IN ({placeholders})",
            tuple(roster),
            fetch=True,
        ) or []
        legacy_rows = _execute_cursor(
            conn,
            f"SELECT CAST(player_id AS varchar), league, season, "
            f"{select_columns}, _batch_id FROM {legacy_table} "
            f"WHERE CAST(player_id AS varchar) IN ({placeholders})",
            tuple(roster),
            fetch=True,
        ) or []
    except Exception as exc:  # noqa: BLE001
        message = str(exc).lower()
        if any(token in message for token in (
            'table_not_found', 'table not found', 'does not exist',
        )):
            return derived
        raise RuntimeError(
            f'bootstrap parity cache unavailable for {endpoint}: {exc}'
        ) from exc

    native_keys: Dict[str, set] = {}
    native_batches: Dict[str, set] = {}
    native_latest: Dict[str, Any] = {}
    for row in native_rows:
        source_id = str(row[0])
        native_keys.setdefault(source_id, set()).add(
            tuple(normalise(value) for value in row[1:-2])
        )
        native_batches.setdefault(source_id, set()).add(normalise(row[-1]))
        seen_at = row[-2]
        if source_id not in native_latest or str(seen_at) > str(
            native_latest[source_id]
        ):
            native_latest[source_id] = seen_at
    legacy_partition_keys: Dict[Tuple[str, str, str], set] = {}
    legacy_partition_batches: Dict[Tuple[str, str, str], set] = {}
    for row in legacy_rows:
        source_id = str(row[0])
        partition = (
            source_id, normalise(row[1]), normalise(row[2]),
        )
        legacy_partition_keys.setdefault(partition, set()).add(
            tuple(normalise(value) for value in row[3:-1])
        )
        legacy_partition_batches.setdefault(partition, set()).add(
            normalise(row[-1])
        )

    def requested_partition(source_id: str) -> Tuple[str, str, str]:
        return source_id, normalise(league), normalise(season_short)

    def bootstrap_proven(source_id: str, keys: set) -> bool:
        batches = native_batches.get(source_id, set())
        return any(
            partition[0] == source_id
            and partition_keys == keys
            and batches <= legacy_partition_batches.get(partition, set())
            for partition, partition_keys in legacy_partition_keys.items()
        )

    for source_id in list(derived):
        partition_ready = bool(
            native_keys.get(source_id)
            and native_keys[source_id] == legacy_partition_keys.get(
                requested_partition(source_id)
            )
        )
        derived[source_id]['legacy_partition_ready'] = partition_ready
        derived[source_id]['needs_legacy_materialization'] = not partition_ready
    for source_id, keys in native_keys.items():
        partition_ready = bool(
            keys == legacy_partition_keys.get(requested_partition(source_id))
        )
        if (
            source_id in derived
            or not keys
            or not bootstrap_proven(source_id, keys)
        ):
            continue
        derived[source_id] = {
            'status': 'success',
            'run_key': 'bootstrap-parity',
            'last_success_at': native_latest.get(source_id),
            'row_count': len(keys),
            'derived': 'bootstrap',
            'legacy_partition_ready': partition_ready,
            'needs_legacy_materialization': not partition_ready,
        }
    return derived


def _load_response_cache() -> tuple[Optional[Dict[str, Any]], Optional[str], Optional[float]]:
    """Return the scope's durable page cache, its path and TTL.

    A league can exceed one cycle's byte cap, so a cycle finishes what the
    previous one paid for instead of re-fetching it. A corrupt cache is an
    ordinary miss.
    """

    path = os.environ.get('TM_RESPONSE_CACHE_PATH')
    if not path:
        return None, None, None
    ttl = float(os.environ.get('TM_RESPONSE_CACHE_TTL_SECONDS') or 0) or None
    try:
        with open(path, encoding='utf-8') as handle:
            cache = json.load(handle)
        if not isinstance(cache, dict):
            cache = {}
    except (OSError, ValueError):
        cache = {}
    return cache, path, ttl


def _persist_response_cache(cache: Optional[Dict[str, Any]], path: Optional[str]) -> None:
    if cache is None or not path:
        return
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        temporary = f'{path}.tmp'
        with open(temporary, 'w', encoding='utf-8') as handle:
            json.dump(cache, handle)
        os.replace(temporary, path)
    except OSError as exc:
        logger.warning('could not persist the scope page cache: %s', exc)


def _pending_checkpoint_roots() -> List[str]:
    """Preferred durable journal root followed by a local emergency fallback."""
    preferred = os.environ.get('TM_PENDING_CHECKPOINT_DIR', '/tmp')
    roots = [preferred]
    if os.path.abspath(preferred) != '/tmp':
        roots.append('/tmp')
    return roots


def _cycle_budget_path(run_key: str, root_override: Optional[str] = None) -> str:
    """Credential-free shared budget ledger path for sequential entity tasks."""
    root = root_override or os.environ.get(
        'TM_CYCLE_BUDGET_DIR', '/opt/airflow/logs/transfermarkt-traffic',
    )
    digest = hashlib.sha256(str(run_key).encode('utf-8')).hexdigest()[:24]
    return os.path.join(root, f'transfermarkt_cycle_{digest}.json')


def _locked_cycle_budget(
    run_key: str,
    limit_bytes: int,
    callback,
) -> Dict[str, Any]:
    """Read/update one cycle ledger under an OS lock; corruption fails closed."""
    import fcntl

    if int(limit_bytes) <= 0:
        raise ValueError('cycle byte budget must be positive')
    path = _cycle_budget_path(run_key)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    lock_path = f'{path}.lock'
    with open(lock_path, 'a+') as lock:
        fcntl.flock(lock.fileno(), fcntl.LOCK_EX)
        try:
            try:
                with open(path) as fh:
                    payload = json.load(fh)
            except FileNotFoundError:
                payload = {
                    'run_key': str(run_key),
                    'limit_bytes': int(limit_bytes),
                    'events': [],
                    'created_at': datetime.now(timezone.utc).isoformat(),
                }
            except (OSError, json.JSONDecodeError) as exc:
                raise RuntimeError(
                    'cycle traffic ledger is unreadable; refusing paid I/O: '
                    f'{_redact_sensitive(exc)}'
                ) from exc
            if (
                payload.get('run_key') != str(run_key)
                or int(payload.get('limit_bytes') or 0) != int(limit_bytes)
            ):
                raise RuntimeError(
                    'cycle traffic ledger identity/budget mismatch; '
                    'refusing paid I/O'
                )
            result, changed = callback(payload)
            if changed:
                tmp_path = f'{path}.{os.getpid()}.tmp'
                payload['updated_at'] = datetime.now(timezone.utc).isoformat()
                try:
                    with open(tmp_path, 'w') as fh:
                        json.dump(payload, fh, default=str)
                        fh.flush()
                        os.fsync(fh.fileno())
                    os.replace(tmp_path, path)
                finally:
                    try:
                        os.unlink(tmp_path)
                    except FileNotFoundError:
                        pass
            return result
        finally:
            fcntl.flock(lock.fileno(), fcntl.LOCK_UN)


def _prepare_cycle_budget(
    run_key: str,
    limit_bytes: int,
    *,
    entity: Optional[str] = None,
    reserve_bytes: Optional[int] = None,
) -> Dict[str, Any]:
    def inspect(payload):
        consumed = sum(
            int(item.get('decoded_response_body_bytes') or 0)
            for item in payload.get('events') or []
        )
        active_reserved = sum(
            int(item.get('reserved_bytes') or 0)
            for item in payload.get('reservations') or []
            if item.get('status') == 'active'
        )
        accounted = consumed + active_reserved
        available = int(limit_bytes) - accounted
        reservation_id = None
        reserved = 0
        if reserve_bytes is not None:
            requested = max(0, int(reserve_bytes))
            reserved = min(requested, max(0, available))
            if reserved <= 0:
                return ({
                    'path': _cycle_budget_path(run_key),
                    'limit_bytes': int(limit_bytes),
                    'consumed_before_bytes': consumed,
                    'active_reserved_before_bytes': active_reserved,
                    'accounted_before_bytes': accounted,
                    'remaining_before_bytes': available,
                    'reservation_id': None,
                    'reserved_bytes': 0,
                }, False)
            reservation_id = str(uuid.uuid4())
            payload.setdefault('reservations', []).append({
                'reservation_id': reservation_id,
                'entity': str(entity or ''),
                'reserved_bytes': reserved,
                'status': 'active',
                'created_at': datetime.now(timezone.utc).isoformat(),
            })
        return ({
            'path': _cycle_budget_path(run_key),
            'limit_bytes': int(limit_bytes),
            'consumed_before_bytes': consumed,
            'active_reserved_before_bytes': active_reserved,
            'accounted_before_bytes': accounted,
            'remaining_before_bytes': available,
            'reservation_id': reservation_id,
            'reserved_bytes': reserved,
            'remaining_after_reservation_bytes': available - reserved,
        }, True)

    status = _locked_cycle_budget(run_key, limit_bytes, inspect)
    if status['remaining_before_bytes'] <= 0 or (
        reserve_bytes is not None and not status.get('reservation_id')
    ):
        raise RuntimeError(
            'Transfermarkt shared cycle byte budget exhausted before paid I/O '
            f"({status['consumed_before_bytes']}/{limit_bytes} bytes)"
        )
    return status


def _record_cycle_traffic(
    run_key: str,
    limit_bytes: int,
    entity: str,
    decoded_bytes: int,
    *,
    reservation_id: Optional[str] = None,
) -> Dict[str, Any]:
    def append(payload):
        if int(decoded_bytes) < 0:
            raise ValueError('decoded response bytes cannot be negative')
        if reservation_id is not None:
            matches = [
                item for item in payload.get('reservations') or []
                if item.get('reservation_id') == reservation_id
                and item.get('status') == 'active'
            ]
            if len(matches) != 1:
                raise RuntimeError(
                    'cycle traffic reservation missing or already settled'
                )
            matches[0]['status'] = 'settled'
            matches[0]['settled_at'] = datetime.now(timezone.utc).isoformat()
            matches[0]['actual_decoded_response_body_bytes'] = int(decoded_bytes)
        events = payload.setdefault('events', [])
        events.append({
            'entity': str(entity),
            'try_number': os.environ.get('AIRFLOW_CTX_TRY_NUMBER', ''),
            'decoded_response_body_bytes': int(decoded_bytes),
            'recorded_at': datetime.now(timezone.utc).isoformat(),
        })
        consumed = sum(
            int(item.get('decoded_response_body_bytes') or 0)
            for item in events
        )
        active_reserved = sum(
            int(item.get('reserved_bytes') or 0)
            for item in payload.get('reservations') or []
            if item.get('status') == 'active'
        )
        accounted = consumed + active_reserved
        return ({
            'path': _cycle_budget_path(run_key),
            'limit_bytes': int(limit_bytes),
            'consumed_after_bytes': consumed,
            'active_reserved_after_bytes': active_reserved,
            'accounted_after_bytes': accounted,
            'remaining_after_bytes': int(limit_bytes) - accounted,
            'exhausted': accounted > int(limit_bytes),
        }, True)

    return _locked_cycle_budget(run_key, limit_bytes, append)


def _pending_checkpoint_path(
    endpoint: str,
    league: str,
    season: int,
    root_override: Optional[str] = None,
) -> str:
    is_global = endpoint in _DERIVED_STATE_SOURCES
    league_scope = 'global' if is_global else league
    season_scope = 'global' if is_global else str(season)
    identity = ':'.join([
        endpoint, league_scope, season_scope, PARSER_VERSION, SCHEMA_VERSION,
    ])
    digest = hashlib.sha256(identity.encode('utf-8')).hexdigest()[:20]
    root = root_override or _pending_checkpoint_roots()[0]
    return os.path.join(root, f'transfermarkt_checkpoint_pending_{digest}.json')


def _load_pending_checkpoint(
    endpoint: str, league: str, season: int,
) -> Tuple[Dict[str, Dict[str, Any]], Optional[Dict[str, Any]]]:
    candidates: List[Tuple[datetime, Dict[str, Any]]] = []
    for root in _pending_checkpoint_roots():
        candidate = _pending_checkpoint_path(
            endpoint, league, season, root_override=root,
        )
        try:
            with open(candidate) as fh:
                item = json.load(fh)
        except FileNotFoundError:
            continue
        except (OSError, json.JSONDecodeError) as exc:
            raise RuntimeError(
                'pending checkpoint is unreadable; refusing paid refetch: '
                f'{_redact_sensitive(exc)}'
            ) from exc
        try:
            created_at = datetime.fromisoformat(str(item['created_at']))
            if created_at.tzinfo is None:
                created_at = created_at.replace(tzinfo=timezone.utc)
            created_at = created_at.astimezone(timezone.utc)
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError('pending checkpoint timestamp is invalid') from exc
        candidates.append((created_at, item))
    if not candidates:
        return {}, None
    created_at, payload = max(candidates, key=lambda candidate: candidate[0])

    expected = {
        'endpoint': endpoint,
        'parser_version': PARSER_VERSION,
        'schema_version': SCHEMA_VERSION,
    }
    if endpoint not in _DERIVED_STATE_SOURCES:
        expected['league'] = league
        expected['season'] = int(season)
    if any(payload.get(key) != value for key, value in expected.items()):
        raise RuntimeError('pending checkpoint identity mismatch')
    state = {}
    live_rows: List[Dict[str, Any]] = []
    expired_source_ids: List[str] = []
    cutoff = datetime.now(timezone.utc) - timedelta(
        days=PENDING_CHECKPOINT_TTL_DAYS,
    )
    for row in payload.get('rows') or []:
        source_id = str(row.get('source_id') or '')
        if not source_id:
            continue
        try:
            committed_at = datetime.fromisoformat(
                str(row.get('committed_at') or payload['created_at'])
            )
            if committed_at.tzinfo is None:
                committed_at = committed_at.replace(tzinfo=timezone.utc)
            committed_at = committed_at.astimezone(timezone.utc)
        except (KeyError, TypeError, ValueError) as exc:
            raise RuntimeError(
                f'pending checkpoint row timestamp is invalid for {source_id}'
            ) from exc
        if committed_at < cutoff:
            expired_source_ids.append(source_id)
            continue
        live_row = dict(row)
        live_row['committed_at'] = committed_at.isoformat()
        live_rows.append(live_row)
        state[source_id] = {
            'status': row.get('status'),
            'run_key': row.get('run_key') or payload.get('run_key'),
            'last_success_at': (
                committed_at if row.get('status') in {
                    'success', 'valid_empty', 'authoritative_empty',
                }
                else None
            ),
            'row_count': int(row.get('row_count') or 0),
            'payload_hash': row.get('payload_hash'),
            'error': row.get('error'),
            'pending': True,
        }
    payload = dict(payload)
    payload['rows'] = live_rows
    if expired_source_ids:
        payload['_expired_source_ids'] = expired_source_ids
    if not live_rows:
        payload['_expired'] = True
    return state, payload


def _write_pending_checkpoint(
    spec: EntitySpec,
    selected: Sequence[str],
    rows: Sequence[Tuple[str, int, Optional[str], Optional[str]]],
    run_key: str,
    league: str,
    season: int,
) -> bool:
    """Durably remember a committed batch while the ops checkpoint is down."""
    endpoint = spec.state_endpoint or spec.name
    now = datetime.now(timezone.utc).isoformat()
    merged: Dict[str, Dict[str, Any]] = {}
    try:
        _, existing = _load_pending_checkpoint(
            endpoint, league, season,
        )
        if existing:
            for item in existing.get('rows') or []:
                if item.get('source_id'):
                    preserved = dict(item)
                    preserved.setdefault(
                        'committed_at', existing.get('created_at', now),
                    )
                    merged[str(item['source_id'])] = preserved
        for source_id, row in zip(selected, rows):
            status, row_count, payload_hash, error = row
            merged[str(source_id)] = {
                'source_id': str(source_id),
                'status': status,
                'row_count': int(row_count),
                'payload_hash': payload_hash,
                'error': error,
                'run_key': run_key,
                'committed_at': now,
            }
        payload = {
            'endpoint': endpoint,
            'league': league,
            'season': int(season),
            'parser_version': PARSER_VERSION,
            'schema_version': SCHEMA_VERSION,
            'run_key': run_key,
            'created_at': now,
            'rows': list(merged.values()),
        }
    except Exception as exc:  # noqa: BLE001
        logger.error(
            'Could not build pending checkpoint payload: %s',
            _redact_sensitive(exc),
        )
        return False

    for index, root in enumerate(_pending_checkpoint_roots()):
        path = _pending_checkpoint_path(
            endpoint, league, season, root_override=root,
        )
        tmp_path = f'{path}.{os.getpid()}.tmp'
        try:
            os.makedirs(root, exist_ok=True)
            with open(tmp_path, 'w') as fh:
                json.dump(payload, fh, default=str)
                fh.flush()
                os.fsync(fh.fileno())
            os.replace(tmp_path, path)
            if index:
                logger.warning(
                    'Pending checkpoint durable directory unavailable; '
                    'using emergency fallback %s', path,
                )
            return True
        except Exception as exc:  # noqa: BLE001
            try:
                os.unlink(tmp_path)
            except OSError:
                pass
            logger.warning(
                'Could not persist pending checkpoint %s: %s',
                path, _redact_sensitive(exc),
            )
    return False


def _clear_pending_checkpoint(endpoint: str, league: str, season: int) -> None:
    for root in _pending_checkpoint_roots():
        path = _pending_checkpoint_path(
            endpoint, league, season, root_override=root,
        )
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
        except OSError as exc:
            logger.warning(
                'Could not clear pending checkpoint %s: %s',
                path, _redact_sensitive(exc),
            )


def _resolve_roster(scraper, league: str, season: int) -> Optional[List[str]]:
    resolver = getattr(scraper, '_resolve_player_ids_from_bronze', None)
    # MagicMock-based legacy tests deliberately do not expose the real method.
    if not callable(getattr(type(scraper), '_resolve_player_ids_from_bronze', None)):
        return None
    try:
        season_short = _canonical_scope_season(league, season)
        return list(resolver(league, season_short, limit=None, window_offset=0))
    except Exception as exc:  # noqa: BLE001
        raise RuntimeError(
            f'roster unavailable; refusing paid endpoint fetch: {exc}'
        ) from exc


def _rotate(values: List[str], limit: int, window_offset: int) -> List[str]:
    if not values:
        return []
    values = sorted(values, key=lambda p: (0, int(p)) if p.isdigit() else (1, p))
    start = (int(window_offset) * int(limit)) % len(values)
    return (values + values)[start:start + limit]


def _select_player_ids(
    scraper,
    spec: EntitySpec,
    league: str,
    season: int,
    limit: int,
    window_offset: int,
    refresh_mode: str,
    run_key: str,
    allow_state_writes: bool,
    legacy_materialization_required: bool = True,
) -> Tuple[Optional[List[str]], int, int, List[str], Dict[str, int]]:
    roster = _resolve_roster(scraper, league, season)
    if roster is None:
        # Compatibility path: let the legacy scraper resolve its own window.
        return None, 0, 0, [], {}
    if not roster:
        raise _EmptyRosterError(
            f'no player ids in Bronze roster for {league}/{season}'
        )
    state = _load_fetch_state(
        scraper,
        spec.state_endpoint or spec.name,
        strict=True,
    )
    endpoint = spec.state_endpoint or spec.name
    pending_state, pending_payload = _load_pending_checkpoint(
        endpoint, league, season,
    )
    if (
        pending_payload
        and not pending_payload.get('_expired')
        and allow_state_writes
        and _flush_pending_checkpoint(
            scraper, spec, pending_payload,
        )
    ):
        _clear_pending_checkpoint(endpoint, league, season)
    # The durable journal describes data that committed after the ops row.  It
    # must therefore override an older failed state; ``setdefault`` would pay
    # for the same endpoint again immediately after a successful replay.
    state.update(pending_state)

    # Historical runs need proof for every globally cached success, not merely
    # an ops-state row.  This both rejects partial native-only commits and tells
    # us when a new requested legacy season can be projected locally from a
    # successful global native+legacy cycle without another paid HTTP request.
    derived_scope = (
        roster
        if refresh_mode == 'historical'
        else [source_id for source_id in roster if source_id not in state]
    )
    derived = _load_data_derived_state(
        scraper, endpoint, league, season, derived_scope,
    )
    hydrate_ids = sorted(
        source_id for source_id, item in derived.items()
        if item.get('needs_legacy_materialization')
    ) if legacy_materialization_required else []
    derived_missing = {
        source_id: item
        for source_id, item in derived.items()
        if source_id not in state
    }
    for source_id, item in derived_missing.items():
        state[source_id] = item

    seeded = 0
    seedable_missing = {
        source_id: item for source_id, item in derived_missing.items()
        if not item.get('needs_legacy_materialization')
    }
    if refresh_mode == 'historical' and seedable_missing and allow_state_writes:
        seed_ids = sorted(seedable_missing)
        seed_rows = [
            ('success', int(seedable_missing[source_id].get('row_count') or 0), None, None)
            for source_id in seed_ids
        ]
        if _persist_fetch_state(
            scraper,
            spec,
            seed_ids,
            seed_rows,
            f'bootstrap-seed:{endpoint}:{SCHEMA_VERSION}',
        ):
            seeded = len(seed_ids)

    if refresh_mode == 'historical':
        valid_empty = {
            pid for pid, item in state.items()
            if item.get('status') in {'valid_empty', 'authoritative_empty'}
        }
        parity_proven = {
            pid for pid, item in derived.items()
            if item.get('status') == 'success'
        }
        committed_native = {
            pid for pid, item in state.items()
            if item.get('status') == 'success'
        }
        cached = valid_empty | (
            parity_proven
            if legacy_materialization_required
            else committed_native | parity_proven
        )
        candidates = [pid for pid in roster if pid not in cached]
        cache_hits = len(set(roster) & cached)
        selected = candidates[:limit]
    else:
        already_this_run = {
            pid for pid, item in state.items()
            if item.get('run_key') == run_key
            and item.get('status') in {
                'success', 'valid_empty', 'authoritative_empty',
            }
        }
        candidates = [pid for pid in roster if pid not in already_this_run]
        cache_hits = len(already_this_run)

    if refresh_mode == 'force':
        selected = _rotate(candidates, limit, window_offset)
        hydrate_ids = []
    elif refresh_mode == 'current':  # oldest successful observation first.
        def _age_key(pid: str):
            last = state.get(pid, {}).get('last_success_at')
            return (last is not None, str(last or ''), (0, int(pid)) if pid.isdigit() else (1, pid))

        selected = sorted(candidates, key=_age_key)[:limit]

    # One cycle buys at most ``limit`` careers of a roster that can run to
    # thousands.  Whoever reads this scope's manifest has to be able to see how
    # much of the roster it actually covers, or a scope that covered 4% of the
    # players reads exactly like one that covered all of them.
    pending = max(0, len(candidates) - len(selected))
    coverage = {
        'roster_size': len(roster),
        'selected': len(selected),
        'pending': pending,
    }
    logger.info(
        'Refresh selection endpoint=%s mode=%s roster=%d selected=%d '
        'cache_hits=%d pending=%d',
        spec.state_endpoint, refresh_mode, len(roster), len(selected),
        cache_hits, pending,
    )
    return selected, cache_hits, seeded, hydrate_ids, coverage


def _frame_hash(frame) -> str:
    try:
        ordered = frame.reindex(sorted(frame.columns), axis=1).astype(str)
        blob = ordered.sort_values(list(ordered.columns)).to_json(
            orient='records', date_format='iso',
        )
    except Exception:  # noqa: BLE001
        blob = str(frame)
    return hashlib.sha256(blob.encode('utf-8')).hexdigest()


def _fetch_outcome_statuses(scraper, endpoint: str) -> Mapping[str, Any]:
    envelope_getter = getattr(scraper, 'get_fetch_outcomes_envelope', None)
    if callable(envelope_getter):
        try:
            envelope = envelope_getter()
            if isinstance(envelope, Mapping):
                outcomes = envelope.get('outcomes')
                if isinstance(outcomes, Mapping):
                    endpoint_outcomes = outcomes.get(endpoint, outcomes)
                    if isinstance(endpoint_outcomes, Mapping):
                        return endpoint_outcomes
        except Exception:  # noqa: BLE001
            pass
    getter = getattr(scraper, 'get_fetch_outcomes', None)
    if not callable(getter):
        return {}
    try:
        outcomes = getter()
        if not isinstance(outcomes, Mapping):
            return {}
        endpoint_outcomes = outcomes.get(endpoint, outcomes)
        return endpoint_outcomes if isinstance(endpoint_outcomes, Mapping) else {}
    except Exception:  # noqa: BLE001
        return {}


def _state_rows(
    scraper,
    spec: EntitySpec,
    selected: Sequence[str],
    authoritative_frame,
    failed_error: Optional[str] = None,
) -> List[Tuple[str, int, Optional[str], Optional[str]]]:
    counts: Dict[str, int] = {}
    hashes: Dict[str, str] = {}
    if authoritative_frame is not None and not getattr(authoritative_frame, 'empty', True):
        for source_id, group in authoritative_frame.groupby(spec.id_column):
            sid = str(source_id)
            counts[sid] = int(len(group))
            hashes[sid] = _frame_hash(group)

    typed = _fetch_outcome_statuses(scraper, spec.state_endpoint or spec.name)
    rows = []
    for sid in selected:
        outcome = typed.get(sid)
        detail = outcome if isinstance(outcome, Mapping) else {}
        kind = (
            detail.get('status')
            if detail else getattr(outcome, 'status', outcome)
        )
        payload_hash = detail.get('payload_hash') or hashes.get(sid)
        error = detail.get('error')
        if failed_error:
            rows.append(('failed', counts.get(sid, 0), hashes.get(sid), failed_error))
            continue
        if sid in counts:
            if kind in {'blocked', 'retry_exhausted', 'schema_error'}:
                rows.append((str(kind), counts[sid], payload_hash, error))
            else:
                rows.append(('success', counts[sid], payload_hash, None))
            continue
        if kind in {'valid_empty', 'authoritative_empty'}:
            rows.append(('authoritative_empty', 0, payload_hash, None))
        else:
            # Never negative-cache an untyped empty: it may be a paid failure.
            rows.append((
                str(kind or 'unverified_empty'), 0, payload_hash, error,
            ))
    return rows


def _persist_fetch_state(
    scraper,
    spec: EntitySpec,
    selected: Sequence[str],
    rows: Sequence[Tuple[str, int, Optional[str], Optional[str]]],
    run_key: str,
) -> bool:
    if not selected or not rows:
        return True
    try:
        conn = scraper._bronze_connection()
        _ensure_fetch_state(conn)
        values = []
        for sid, (status, row_count, payload_hash, error) in zip(selected, rows):
            def q(value):
                if value is None:
                    return 'NULL'
                return "'" + str(value).replace("'", "''") + "'"

            values.append(
                '(' + ', '.join([
                    q(spec.state_endpoint), q(sid), q(PARSER_VERSION),
                    q(SCHEMA_VERSION), q(status), q(run_key),
                    str(int(row_count)), q(payload_hash), q(error),
                ]) + ')'
            )
        sql = f"""
MERGE INTO {FETCH_STATE_TABLE} t
USING (
    VALUES {', '.join(values)}
) s(endpoint, source_id, parser_version, schema_version, status, run_key,
    row_count, payload_hash, error)
ON t.endpoint = s.endpoint
AND t.source_id = s.source_id
AND t.parser_version = s.parser_version
AND t.schema_version = s.schema_version
WHEN MATCHED THEN UPDATE SET
    status = s.status,
    run_key = s.run_key,
    last_attempt_at = current_timestamp,
    last_success_at = CASE
        WHEN s.status IN ('success', 'valid_empty', 'authoritative_empty')
          THEN current_timestamp
        ELSE t.last_success_at
    END,
    row_count = s.row_count,
    payload_hash = s.payload_hash,
    error = s.error
WHEN NOT MATCHED THEN INSERT (
    endpoint, source_id, parser_version, schema_version, status, run_key,
    first_attempt_at, last_attempt_at, last_success_at,
    row_count, payload_hash, error
) VALUES (
    s.endpoint, s.source_id, s.parser_version, s.schema_version, s.status, s.run_key,
    current_timestamp, current_timestamp,
    CASE WHEN s.status IN ('success', 'valid_empty', 'authoritative_empty')
         THEN current_timestamp END,
    s.row_count, s.payload_hash, s.error
)
"""
        _execute_cursor(conn, sql)
        return True
    except Exception as exc:  # noqa: BLE001
        # Data is committed already. Telemetry/checkpoint failure is loud in the
        # result but must not turn a successful idempotent Bronze write into a
        # retry that pays for the same HTTP calls again.
        logger.error(
            'Could not persist Transfermarkt fetch state: %s',
            _redact_sensitive(exc),
        )
        return False


def _flush_pending_checkpoint(
    scraper,
    spec: EntitySpec,
    payload: Mapping[str, Any],
) -> bool:
    """Replay a local post-commit checkpoint once the ops table is healthy."""
    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for item in payload.get('rows') or []:
        run_key = str(item.get('run_key') or payload.get('run_key') or 'pending')
        grouped.setdefault(run_key, []).append(dict(item))
    for run_key, items in grouped.items():
        selected = [str(item['source_id']) for item in items]
        rows = [
            (
                str(item.get('status') or 'unverified_empty'),
                int(item.get('row_count') or 0),
                item.get('payload_hash'),
                item.get('error'),
            )
            for item in items
        ]
        if not _persist_fetch_state(scraper, spec, selected, rows, run_key):
            return False
    logger.info(
        'Recovered pending Transfermarkt checkpoint endpoint=%s rows=%d',
        spec.state_endpoint, sum(len(items) for items in grouped.values()),
    )
    return True


def _commit_checkpoint_or_pending(
    scraper,
    spec: EntitySpec,
    selected: Sequence[str],
    rows: Sequence[Tuple[str, int, Optional[str], Optional[str]]],
    run_key: str,
    league: str,
    season: int,
) -> str:
    """Advance ops state or leave a recoverable local post-commit journal."""
    if not selected:
        return 'cache_complete'
    endpoint = spec.state_endpoint or spec.name
    if _persist_fetch_state(scraper, spec, selected, rows, run_key):
        _clear_pending_checkpoint(endpoint, league, season)
        return 'success'
    if _write_pending_checkpoint(
        spec, selected, rows, run_key, league, season,
    ):
        return 'committed_checkpoint_pending'
    return 'checkpoint_unrecoverable'


def _native_available(scraper, method_name: str) -> bool:
    """Compatibility gate retained through the 30-day rollback window."""
    return callable(getattr(type(scraper), method_name, None))


def _align_batch_ids(scraper, frames: Mapping[str, Any]) -> Dict[str, Any]:
    """Stamp every native/legacy projection with one shared scrape batch id."""
    batch_id = str(getattr(scraper, '_batch_id', '') or '')
    if not batch_id:
        return dict(frames)
    aligned = {}
    for key, frame in frames.items():
        if frame is None or getattr(frame, 'empty', True):
            aligned[key] = frame
            continue
        copy = frame.copy()
        copy['_batch_id'] = batch_id
        aligned[key] = copy
    return aligned


def _load_coach_memberships(scraper, league: str, season: int):
    """Load the player crawl's club scope so coaches skip a paid listing."""
    import pandas as pd

    season_short = _canonical_scope_season(league, season)
    conn = scraper._bronze_connection()
    rows = _execute_cursor(
        conn,
        "SELECT DISTINCT club_id, club_slug, club_name "
        "FROM iceberg.bronze.transfermarkt_squad_memberships "
        "WHERE league = ? AND season = ? "
        "ORDER BY club_id",
        (league, season_short),
        fetch=True,
    ) or []
    memberships = pd.DataFrame(rows, columns=['club_id', 'club_slug', 'club_name'])
    if memberships.empty:
        raise RuntimeError(
            'coach club scope is empty; refusing duplicate league-listing fetch'
        )
    return memberships


def _as_utc(value: Any) -> Optional[datetime]:
    if value is None:
        return None
    if isinstance(value, datetime):
        parsed = value
    else:
        try:
            parsed = datetime.fromisoformat(str(value).replace('Z', '+00:00'))
        except ValueError:
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)


def _select_coach_memberships(
    scraper,
    memberships,
    league: str,
    season: int,
    refresh_mode: str,
    run_key: str,
    ttl_days: int,
    allow_state_writes: bool,
):
    """Select only missing/stale club history endpoints under a persisted TTL."""
    spec = COACH_HISTORY_CHECKPOINT_SPEC
    roster = sorted(memberships['club_id'].dropna().astype(str).unique())
    state = _load_fetch_state(scraper, spec.state_endpoint, strict=True)
    pending_state, pending_payload = _load_pending_checkpoint(
        spec.state_endpoint, league, season,
    )
    if (
        pending_payload
        and not pending_payload.get('_expired')
        and allow_state_writes
        and _flush_pending_checkpoint(
            scraper, spec, pending_payload,
        )
    ):
        _clear_pending_checkpoint(spec.state_endpoint, league, season)
    state.update(pending_state)

    untracked = [source_id for source_id in roster if source_id not in state]
    derived = _load_data_derived_state(
        scraper, spec.state_endpoint, league, season, untracked,
    )
    derived_missing = {
        source_id: item for source_id, item in derived.items()
        if source_id not in state
    }
    for source_id, item in derived_missing.items():
        state[source_id] = item

    seeded = 0
    if refresh_mode == 'historical' and derived_missing and allow_state_writes:
        source_ids = sorted(derived_missing)
        rows = [
            ('success', int(derived_missing[source_id].get('row_count') or 0), None, None)
            for source_id in source_ids
        ]
        if _persist_fetch_state(
            scraper,
            spec,
            source_ids,
            rows,
            f'bootstrap-seed:{spec.state_endpoint}:{SCHEMA_VERSION}',
        ):
            seeded = len(source_ids)

    already_this_run = {
        source_id for source_id, item in state.items()
        if item.get('run_key') == run_key
        and item.get('status') in {
            'success', 'valid_empty', 'authoritative_empty',
        }
    }
    candidates = [club_id for club_id in roster if club_id not in already_this_run]
    if refresh_mode == 'historical':
        cached = {
            source_id for source_id, item in state.items()
            if item.get('status') in {
                'success', 'valid_empty', 'authoritative_empty',
            }
        }
        selected = [club_id for club_id in candidates if club_id not in cached]
    elif refresh_mode == 'force':
        selected = candidates
    else:
        cutoff = datetime.now(timezone.utc) - timedelta(days=int(ttl_days))
        selected = []
        for club_id in candidates:
            item = state.get(club_id, {})
            last_success = _as_utc(item.get('last_success_at'))
            if (
                item.get('status') not in {
                    'success', 'valid_empty', 'authoritative_empty',
                }
                or last_success is None
                or last_success <= cutoff
            ):
                selected.append(club_id)

    selected_set = set(selected)
    selected_memberships = memberships[
        memberships['club_id'].astype(str).isin(selected_set)
    ].copy()
    cache_hits = len(roster) - len(selected)
    logger.info(
        'Coach-history selection mode=%s ttl_days=%d clubs=%d selected=%d cache_hits=%d',
        refresh_mode, ttl_days, len(roster), len(selected), cache_hits,
    )
    cached = [club_id for club_id in roster if club_id not in selected_set]
    return selected_memberships, selected, cached, cache_hits, seeded


def _query_dataframe(conn, sql: str, params: Sequence[Any]):
    import pandas as pd

    cur = conn.cursor()
    try:
        cur.execute(sql, tuple(params))
        columns = [item[0] for item in (cur.description or [])]
        rows = cur.fetchall()
        return pd.DataFrame(rows, columns=columns)
    finally:
        try:
            cur.close()
        except Exception:  # noqa: BLE001
            pass


_CAREER_CACHE_COLUMNS = {
    ENTITY_MV_HISTORY: (
        'market_value_points',
        'iceberg.bronze.transfermarkt_market_value_points',
        'market_value_points',
        (
            'player_id', 'mv_date', 'value_eur', 'club_name', 'age', 'mv_raw',
            '_source', '_entity_type', '_ingested_at', '_batch_id',
        ),
        ('player_id', 'mv_date'),
    ),
    ENTITY_TRANSFERS: (
        'transfer_events',
        'iceberg.bronze.transfermarkt_transfer_events',
        'transfer_events',
        (
            'transfer_id', 'player_id', 'transfer_date', 'event_season',
            'from_club_id', 'from_club_name', 'to_club_id', 'to_club_name',
            'fee_text', 'fee_eur', 'market_value_eur', 'is_upcoming',
            '_source', '_entity_type', '_ingested_at', '_batch_id',
        ),
        ('transfer_id',),
    ),
}


def _load_cached_career_frames(
    scraper,
    spec: EntitySpec,
    source_ids: Sequence[str],
) -> Dict[str, Any]:
    """Read only manifest- or bootstrap-parity-proven global career rows."""
    import pandas as pd

    source_ids = [str(value) for value in dict.fromkeys(source_ids)]
    key, table, entity, columns, _ = _CAREER_CACHE_COLUMNS[spec.name]
    legacy_output = next(output for output in spec.outputs if output.is_legacy)
    legacy_table = f'iceberg.bronze.{legacy_output.table_name}'
    if not source_ids:
        return {key: pd.DataFrame(columns=columns)}
    placeholders = ', '.join('?' for _ in source_ids)
    frame = _query_dataframe(
        scraper._bronze_connection(),
        f"SELECT {', '.join('n.' + column for column in columns)} "
        f"FROM {table} n "
        f"WHERE CAST(n.player_id AS varchar) IN ({placeholders}) "
        f"AND (EXISTS (SELECT 1 FROM {DUAL_WRITE_MANIFEST_TABLE} m "
        "WHERE m.entity = ? AND m.status = 'success' "
        "AND m.legacy_batch_id = m.native_batch_id "
        "AND m.legacy_rows = m.native_rows "
        "AND m.legacy_hash = m.native_hash "
        "AND m.native_batch_id = n._batch_id) "
        f"OR EXISTS (SELECT 1 FROM {legacy_table} l "
        "WHERE CAST(l.player_id AS varchar) = CAST(n.player_id AS varchar) "
        "AND l._batch_id = n._batch_id))",
        (*source_ids, entity),
    )
    loaded = set(frame['player_id'].dropna().astype(str)) if not frame.empty else set()
    missing = sorted(set(source_ids) - loaded)
    if missing:
        raise RuntimeError(
            'parity-proven career cache changed before materialisation; '
            f'missing source ids: {missing}'
        )
    return {key: frame}


def _merge_career_cache_frames(
    scraper,
    spec: EntitySpec,
    fetched: Mapping[str, Any],
    cached: Mapping[str, Any],
    league: str,
    season: int,
) -> Dict[str, Any]:
    """Combine fresh and cached native rows and rebuild one legacy partition."""
    import pandas as pd

    key, _, _, columns, natural_key = _CAREER_CACHE_COLUMNS[spec.name]
    parts = [
        frame for frame in (cached.get(key), fetched.get(key))
        if frame is not None
    ]
    native = (
        pd.concat(parts, ignore_index=True)
        if parts else pd.DataFrame(columns=columns)
    )
    if not native.empty:
        native = native.drop_duplicates(list(natural_key), keep='last')
    scope = _scope_season(league, season)
    if spec.name == ENTITY_MV_HISTORY:
        legacy = scraper.materialize_legacy_market_value_history(
            native, league, scope['season_year'], scope['season_format'],
        )
        return {
            'market_value_points': native,
            'legacy_market_value_history': legacy,
        }
    legacy = scraper.materialize_legacy_transfers(
        native, league, scope['season_year'], scope['season_format'],
    )
    return {'transfer_events': native, 'legacy_transfers': legacy}


def _load_cached_coach_data(scraper, club_ids: Sequence[str]) -> Dict[str, Any]:
    """Load global coach facts for cache-only season materialisation."""
    import pandas as pd

    club_ids = [str(value) for value in dict.fromkeys(club_ids)]
    profile_columns = [
        'coach_id', 'coach_slug', 'name', 'dob', 'nationality',
        '_source', '_entity_type', '_ingested_at', '_batch_id',
    ]
    stint_columns = [
        'club_id', 'club_name', 'coach_id', 'coach_slug', 'name', 'role',
        'appointed_date', 'left_date', '_source', '_entity_type',
        '_ingested_at', '_batch_id',
    ]
    if not club_ids:
        return {
            'profiles': pd.DataFrame(columns=profile_columns),
            'stints': pd.DataFrame(columns=stint_columns),
        }
    placeholders = ', '.join('?' for _ in club_ids)
    conn = scraper._bronze_connection()
    stints = _query_dataframe(
        conn,
        "SELECT club_id,club_name,coach_id,coach_slug,name,role,"
        "appointed_date,left_date,_source,_entity_type,_ingested_at,_batch_id "
        "FROM iceberg.bronze.transfermarkt_coach_stints "
        f"WHERE CAST(club_id AS varchar) IN ({placeholders})",
        club_ids,
    )
    profiles = _query_dataframe(
        conn,
        "SELECT coach_id,coach_slug,name,dob,nationality,_source,_entity_type,"
        "_ingested_at,_batch_id "
        "FROM iceberg.bronze.transfermarkt_coach_profiles "
        "WHERE coach_id IN (SELECT DISTINCT coach_id FROM "
        "iceberg.bronze.transfermarkt_coach_stints "
        f"WHERE CAST(club_id AS varchar) IN ({placeholders}))",
        club_ids,
    )
    return {'profiles': profiles, 'stints': stints}


def _merge_coach_cache_frames(
    scraper,
    fetched: Mapping[str, Any],
    cached: Mapping[str, Any],
    league: str,
    season: int,
) -> Dict[str, Any]:
    import pandas as pd

    profiles = pd.concat(
        [cached.get('profiles'), fetched.get('profiles')], ignore_index=True,
    ).drop_duplicates('coach_id', keep='last')
    stints = pd.concat(
        [cached.get('stints'), fetched.get('stints')], ignore_index=True,
    ).drop_duplicates(
        ['club_id', 'coach_id', 'appointed_date', 'left_date'], keep='last',
    )
    # Both the scraper and this merge project the same league-season, so both
    # must date its window the same way.  Defaulting the format here made a
    # calendar season a split one and the two projections disagreed.
    scope = _scope_season(league, season)
    legacy = scraper.materialize_legacy_coaches(
        profiles, stints, league, scope['season_year'], scope['season_format'],
    )
    return {
        'profiles': profiles,
        'stints': stints,
        'legacy_coaches': legacy,
    }


def _read_frames(
    scraper,
    spec: EntitySpec,
    league: str,
    season: int,
    limit: Optional[int],
    window_offset: int,
    selected: Optional[List[str]],
    native_dual_write: bool,
    coach_memberships=None,
    write_mode: Optional[str] = None,
) -> Tuple[Dict[str, Any], str, bool]:
    """Return output frames, authoritative key and whether native was used."""
    mode = _normalise_write_mode(
        write_mode, native_dual_write=native_dual_write,
    )
    common = {'league': league, 'season': int(season)}
    if spec.state_endpoint:
        if selected is not None:
            common['player_ids'] = selected
        common.update(limit=limit, window_offset=window_offset)
    elif limit is not None:
        common['limit'] = limit

    use_native = (
        mode in {'dual', 'native-only'}
        and _native_available(scraper, spec.native_reader)
    )
    if mode == 'native-only' and not use_native:
        raise RuntimeError(
            f'native-only writer mode requires {spec.native_reader}; '
            'legacy fallback is forbidden after cleanup'
        )
    if use_native and spec.name == ENTITY_COACHES:
        common['memberships'] = (
            coach_memberships
            if coach_memberships is not None
            else _load_coach_memberships(scraper, league, season)
        )
    if use_native and spec.name in {ENTITY_PLAYERS, ENTITY_COACHES}:
        bundle = getattr(scraper, spec.native_reader)(**common)
        if not isinstance(bundle, Mapping):
            raise TypeError(f'{spec.native_reader} must return a mapping')
        frames = dict(bundle)
        if spec.name == ENTITY_PLAYERS:
            if mode == 'dual' and 'legacy_players' not in frames:
                frames['legacy_players'] = scraper.materialize_legacy_players(
                    frames['memberships'], frames['attribute_observations'],
                )
            if mode == 'native-only':
                frames.pop('legacy_players', None)
                authoritative = 'memberships'
            else:
                authoritative = 'legacy_players'
        else:
            if mode == 'dual' and 'legacy_coaches' not in frames:
                scope = _scope_season(league, season)
                frames['legacy_coaches'] = scraper.materialize_legacy_coaches(
                    frames['profiles'], frames['stints'], league,
                    scope['season_year'], scope['season_format'],
                )
            if mode == 'native-only':
                frames.pop('legacy_coaches', None)
                authoritative = 'stints'
            else:
                authoritative = 'legacy_coaches'
        return frames, authoritative, True

    if use_native and spec.name == ENTITY_MV_HISTORY:
        points = getattr(scraper, spec.native_reader)(**common)
        if mode == 'native-only':
            return {'market_value_points': points}, 'market_value_points', True
        scope = _scope_season(league, season)
        legacy = scraper.materialize_legacy_market_value_history(
            points, league, scope['season_year'], scope['season_format'],
        )
        return {
            'market_value_points': points,
            'legacy_market_value_history': legacy,
        }, 'market_value_points', True

    if use_native and spec.name == ENTITY_TRANSFERS:
        events = getattr(scraper, spec.native_reader)(**common)
        if mode == 'native-only':
            return {'transfer_events': events}, 'transfer_events', True
        scope = _scope_season(league, season)
        legacy = scraper.materialize_legacy_transfers(
            events, league, scope['season_year'], scope['season_format'],
        )
        return {
            'transfer_events': events,
            'legacy_transfers': legacy,
        }, 'transfer_events', True

    logger.warning(
        'Native reader %s unavailable/disabled; using legacy-only rollback path',
        spec.native_reader,
    )
    legacy = getattr(scraper, spec.legacy_reader)(**common)
    legacy_key = next(output.key for output in spec.outputs if output.is_legacy)
    return {legacy_key: legacy}, legacy_key, False


def _save_frames(
    scraper,
    spec: EntitySpec,
    frames: Mapping[str, Any],
    force_replace: bool,
    results: Dict[str, Any],
) -> None:
    for output in spec.outputs:
        if output.key not in frames:
            continue
        frame = frames[output.key]
        if frame is None or getattr(frame, 'empty', True):
            previous = results['outputs'].get(output.key, {})
            applicability = previous.get('applicability_status')
            results['outputs'][output.key] = {
                'rows': 0,
                'table': (
                    f'iceberg.bronze.{output.table_name}'
                    if applicability in {
                        'authoritative_empty', 'not_applicable',
                    }
                    else None
                ),
                'applicability_status': applicability,
            }
            continue
        guard = None if force_replace or not output.guard_key else _MIN_REPLACE_RATIO
        table_path = scraper.save_to_iceberg(
            df=frame,
            table_name=output.table_name,
            partition_cols=list(output.partition_cols),
            replace_partitions=list(output.replace_keys) or None,
            min_replace_ratio=guard,
            replace_guard_key=output.guard_key,
        )
        results['tables'].append(table_path)
        results['outputs'][output.key] = {
            'rows': int(len(frame)),
            'table': table_path,
            'applicability_status': 'ok',
        }


def _frame_output_summary(frame) -> Dict[str, Any]:
    """Expose only an explicit terminal empty status carried by a frame."""

    rows = int(len(frame)) if frame is not None else 0
    if rows > 0:
        status = 'ok'
    else:
        attrs = getattr(frame, 'attrs', {}) or {}
        raw_status = attrs.get('applicability_status', attrs.get('fetch_status'))
        status = (
            str(raw_status)
            if raw_status in {'authoritative_empty', 'not_applicable'}
            else None
        )
    return {'rows': rows, 'table': None, 'applicability_status': status}


_PLAYER_SCOPE_DQ_OUTPUTS = {
    'memberships': 'squad_memberships',
    'attribute_observations': 'player_attribute_observations',
    'contract_observations': 'player_contract_observations',
}


def _native_scope_capture(scraper) -> Optional[Dict[str, Any]]:
    getter = getattr(scraper, 'get_scope_capture', None)
    if not callable(getter):
        return None
    value = getter()
    return dict(value) if isinstance(value, Mapping) else None


def _validate_native_player_scope_capture(
    capture: Optional[Mapping[str, Any]],
    *,
    competition_record,
    outputs: Mapping[str, Mapping[str, Any]],
    current: bool,
) -> Dict[str, Any]:
    """Block a native Bronze write unless participant evidence is complete."""

    if not isinstance(capture, Mapping):
        raise RuntimeError('native scope participant capture evidence is missing')
    exact_identity = {
        'competition_id': competition_record.competition_id,
        'edition_id': str(os.environ.get('TM_EDITION_ID', '')).strip(),
        'scope_id': str(os.environ.get('TM_SCOPE_ID', '')).strip(),
        'competition_type': competition_record.competition_type.value,
        'team_type': competition_record.team_type.value,
        'gender': 'men',
        'age_category': 'senior',
    }
    for field, expected in exact_identity.items():
        if not expected or str(capture.get(field, '')).strip() != expected:
            raise RuntimeError(
                f'native scope participant capture {field} mismatch'
            )
    expected_count = os.environ.get('TM_EXPECTED_PARTICIPANT_COUNT', '').strip()
    if expected_count:
        expected_teams = capture.get('expected_team_ids')
        if (
            not isinstance(expected_teams, Sequence)
            or isinstance(expected_teams, (str, bytes))
            or len(expected_teams) != int(expected_count)
        ):
            actual = (
                len(expected_teams)
                if isinstance(expected_teams, Sequence)
                and not isinstance(expected_teams, (str, bytes))
                else 'invalid'
            )
            raise RuntimeError(
                'native scope listing participant count differs from registry: '
                f'{actual}/{expected_count}'
            )
    statuses: Dict[str, str] = {}
    for output_key, entity in _PLAYER_SCOPE_DQ_OUTPUTS.items():
        output = outputs.get(output_key)
        if not isinstance(output, Mapping):
            raise RuntimeError(f'native scope DQ output is missing: {output_key}')
        rows = int(output.get('rows', -1))
        status = 'ok' if rows > 0 else output.get('applicability_status')
        statuses[entity] = str(status or '')
    try:
        return validate_scope_capture(
            input_from_capture(
                capture,
                entity_statuses=statuses,
                current=current,
            ),
            expected_entities=statuses,
        )
    except ScopeDQError as exc:
        raise RuntimeError(f'native scope participant DQ failed: {exc}') from exc


def _valid_empty_ids(scraper, spec: EntitySpec, selected: Sequence[str]) -> List[str]:
    typed = _fetch_outcome_statuses(scraper, spec.state_endpoint or spec.name)
    empty = []
    for source_id in selected:
        outcome = typed.get(source_id)
        status = (
            outcome.get('status') if isinstance(outcome, Mapping)
            else getattr(outcome, 'status', outcome)
        )
        if status in {'valid_empty', 'authoritative_empty'}:
            empty.append(str(source_id))
    return empty


def _delete_valid_empty_rows(
    scraper,
    spec: EntitySpec,
    source_ids: Sequence[str],
    league: str,
    season: int,
    source_key: Optional[str] = None,
) -> Dict[str, str]:
    """Authoritative empty responses remove stale rows for those exact keys."""
    if not source_ids:
        return {}
    quoted_ids = ', '.join(
        "'" + str(source_id).replace("'", "''") + "'"
        for source_id in source_ids
    )
    season_short = _canonical_scope_season(league, season)
    conn = scraper._bronze_connection()
    committed = {}
    source_key = source_key or spec.id_column
    for output in spec.outputs:
        table = f'iceberg.bronze.{output.table_name}'
        if source_key == 'club_id' and output.key == 'profiles':
            # An empty club history has no profile ownership to delete. Keeping
            # global profiles preserves the immutable bio cache.
            committed[output.key] = table
            continue
        target_key = (
            'current_club_id'
            if source_key == 'club_id' and output.is_legacy
            else source_key
        )
        where = f"{target_key} IN ({quoted_ids})"
        if output.is_legacy and source_key == 'club_id':
            safe_league = league.replace("'", "''")
            where += (
                f" AND league = '{safe_league}' AND season = '{season_short}'"
            )
        try:
            _execute_cursor(conn, f'DELETE FROM {table} WHERE {where}')
        except Exception as exc:  # noqa: BLE001
            message = str(exc).lower()
            if any(token in message for token in (
                'table_not_found', 'table not found', 'does not exist',
            )):
                # There cannot be stale rows in an absent table; the delete is
                # an authoritative no-op for this key set.
                committed[output.key] = table
                continue
            raise
        committed[output.key] = table
    return committed


def _delete_removed_coach_clubs(
    scraper, memberships, league: str, season: int,
) -> None:
    """Remove legacy season rows for clubs no longer in the squad membership."""
    club_ids = sorted(memberships['club_id'].dropna().astype(str).unique())
    if not club_ids:
        raise RuntimeError('cannot reconcile legacy coaches without club memberships')
    quoted = ', '.join(
        "'" + club_id.replace("'", "''") + "'" for club_id in club_ids
    )
    safe_league = league.replace("'", "''")
    season_short = _canonical_scope_season(league, season)
    conn = scraper._bronze_connection()
    try:
        _execute_cursor(
            conn,
            "DELETE FROM iceberg.bronze.transfermarkt_coaches "
            f"WHERE league = '{safe_league}' AND season = '{season_short}' "
            f"AND current_club_id NOT IN ({quoted})",
        )
    except Exception as exc:  # noqa: BLE001
        if any(token in str(exc).lower() for token in (
            'table_not_found', 'table not found', 'does not exist',
        )):
            return
        raise


def _delete_empty_season_coach_rows(
    scraper,
    selected_club_ids: Sequence[str],
    legacy_frame,
    league: str,
    season: int,
) -> None:
    """Delete stale seasonal coaches when history has no overlapping stint."""
    materialized = set()
    if legacy_frame is not None and not getattr(legacy_frame, 'empty', True):
        materialized = set(
            legacy_frame['current_club_id'].dropna().astype(str).unique()
        )
    missing = sorted(set(map(str, selected_club_ids)) - materialized)
    if not missing:
        return
    quoted = ', '.join("'" + value.replace("'", "''") + "'" for value in missing)
    safe_league = league.replace("'", "''")
    season_short = _canonical_scope_season(league, season)
    conn = scraper._bronze_connection()
    try:
        _execute_cursor(
            conn,
            "DELETE FROM iceberg.bronze.transfermarkt_coaches "
            f"WHERE league = '{safe_league}' AND season = '{season_short}' "
            f"AND current_club_id IN ({quoted})",
        )
    except Exception as exc:  # noqa: BLE001
        if any(token in str(exc).lower() for token in (
            'table_not_found', 'table not found', 'does not exist',
        )):
            return
        raise


def _frame_batch_id(frame, run_key: str) -> str:
    if frame is not None and not getattr(frame, 'empty', True):
        if '_batch_id' in frame.columns:
            values = frame['_batch_id'].dropna().astype(str).unique()
            if len(values):
                return str(values[0])
    return hashlib.sha256(run_key.encode('utf-8')).hexdigest()[:32]


_MANIFEST_COMPATIBILITY = {
    'memberships': {
        'entity': 'squad_memberships',
        'native': (
            'league', 'season', 'club_id', 'club_name', 'player_id',
            'player_slug', 'player_name',
        ),
        'legacy': (
            'league', 'season', 'current_club_id', 'current_club_name',
            'player_id', 'player_slug', 'name',
        ),
    },
    'attribute_observations': {
        'entity': 'player_attribute_observations',
        # observed_at is the observation grain, but legacy is a snapshot. The
        # transition parity intentionally compares the compatible membership
        # projection only.
        'native': (
            'player_id', 'player_slug', 'name', 'position', 'dob', 'age',
            'height_cm', 'foot', 'nationality', 'contract_until',
            'market_value_eur', 'league', 'season', 'club_id', 'club_name',
        ),
        'legacy': (
            'player_id', 'player_slug', 'name', 'position', 'dob', 'age',
            'height_cm', 'foot', 'nationality', 'contract_until',
            'market_value_eur', 'league', 'season', 'current_club_id',
            'current_club_name',
        ),
    },
    'contract_observations': {
        'entity': 'player_contract_observations',
        'native': (
            'player_id', 'contract_until', 'team_id', 'team_name',
        ),
        'legacy': (
            'player_id', 'contract_until', 'current_club_id',
            'current_club_name',
        ),
    },
    'market_value_points': {
        'entity': 'market_value_points',
        'native': (
            'player_id', 'mv_date', 'value_eur', 'club_name', 'age', 'mv_raw',
        ),
        'legacy': (
            'player_id', 'mv_date', 'value_eur', 'club_name', 'age', 'mv_raw',
        ),
    },
    'transfer_events': {
        'entity': 'transfer_events',
        'native': (
            'player_id', 'transfer_date', 'from_club_id', 'from_club_name',
            'to_club_id', 'to_club_name', 'fee_text', 'is_upcoming',
            'fee_eur', 'market_value_eur',
        ),
        'legacy': (
            'player_id', 'transfer_date', 'from_club_id', 'from_club_name',
            'to_club_id', 'to_club_name', 'fee_text', 'is_upcoming',
            'fee_eur', 'market_value_eur',
        ),
    },
    'profiles': {
        'entity': 'coach_profiles',
        'native': ('coach_id', 'coach_slug', 'name', 'dob', 'nationality'),
        'legacy': ('coach_id', 'coach_slug', 'name', 'dob', 'nationality'),
    },
    'stints': {
        'entity': 'coach_stints',
        'native': (
            'club_id', 'club_name', 'coach_id', 'coach_slug', 'name', 'role',
        ),
        'legacy': (
            'current_club_id', 'current_club_name', 'coach_id', 'coach_slug',
            'name', 'role',
        ),
    },
}


def _compatibility_keys(frame, columns: Sequence[str]) -> List[Tuple[str, ...]]:
    """Sorted DISTINCT normalized key tuples with a stable NULL sentinel."""
    def normalise(value: Any) -> str:
        if value is None:
            return '__NULL__'
        try:
            if bool(value != value):  # NaN/NaT without importing pandas here.
                return '__NULL__'
        except (TypeError, ValueError):
            pass
        if str(value) in {'nan', 'NaT', '<NA>'}:
            return '__NULL__'
        return str(value)

    if frame is None:
        values: List[Tuple[str, ...]] = []
    else:
        missing = [column for column in columns if column not in frame.columns]
        if missing:
            raise ValueError(f'manifest compatibility columns missing: {missing}')
        projected = frame[list(columns)].drop_duplicates()
        values = sorted(
            tuple(normalise(value) for value in row)
            for row in projected.itertuples(index=False, name=None)
        )
    return values


def _compatibility_fingerprint(frame, columns: Sequence[str]) -> Tuple[int, str]:
    values = _compatibility_keys(frame, columns)
    blob = json.dumps(values, ensure_ascii=False, separators=(',', ':'))
    return len(values), hashlib.sha256(blob.encode('utf-8')).hexdigest()


def _persist_dual_write_manifest(
    scraper,
    spec: EntitySpec,
    frames: Mapping[str, Any],
    results: Dict[str, Any],
    run_key: str,
    league: str,
    season: int,
) -> Dict[str, Any]:
    """Persist one compatibility-checked row per native↔legacy pair."""
    legacy_specs = [output for output in spec.outputs if output.is_legacy]
    native_specs = [output for output in spec.outputs if not output.is_legacy]
    if len(legacy_specs) != 1 or not native_specs:
        raise RuntimeError(f'invalid dual-write output contract for {spec.name}')
    legacy = legacy_specs[0]
    legacy_frame = frames.get(legacy.key)
    legacy_batch_id = _frame_batch_id(legacy_frame, run_key)

    committed_keys = {
        key for key, item in results.get('outputs', {}).items()
        if item.get('table')
    }
    expected_keys = {output.key for output in spec.outputs}
    if not expected_keys.issubset(committed_keys):
        missing = sorted(expected_keys - committed_keys)
        raise RuntimeError(f'dual-write outputs not committed: {missing}')

    conn = scraper._bronze_connection()
    _execute_cursor(conn, 'CREATE SCHEMA IF NOT EXISTS iceberg.ops')
    _execute_cursor(
        conn,
        f"CREATE TABLE IF NOT EXISTS {DUAL_WRITE_MANIFEST_TABLE} ("
        "cycle_id varchar, league varchar, season integer, entity varchar, "
        "legacy_table varchar, native_table varchar, "
        "legacy_batch_id varchar, native_batch_id varchar, "
        "legacy_rows bigint, native_rows bigint, legacy_hash varchar, "
        "native_hash varchar, status varchar, "
        "committed_at timestamp(6)) WITH (format = 'PARQUET')",
    )
    # Existing review environments may already have the pre-scope manifest.
    # Additive evolution keeps those rows readable while making every new
    # readiness decision explicitly league/season scoped.
    _execute_cursor(
        conn,
        f"ALTER TABLE {DUAL_WRITE_MANIFEST_TABLE} "
        "ADD COLUMN IF NOT EXISTS league varchar",
    )
    _execute_cursor(
        conn,
        f"ALTER TABLE {DUAL_WRITE_MANIFEST_TABLE} "
        "ADD COLUMN IF NOT EXISTS season integer",
    )

    def q(value):
        if value is None:
            return 'NULL'
        return "'" + str(value).replace("'", "''") + "'"

    manifest_rows = []
    for native in native_specs:
        native_frame = frames.get(native.key)
        contract = _MANIFEST_COMPATIBILITY[native.key]
        applicability = _frame_output_summary(native_frame)[
            'applicability_status'
        ]
        comparison_legacy_frame = legacy_frame
        if applicability == 'not_applicable' and legacy_frame is not None:
            # National-team contracts are outside the source contract.  Their
            # compatibility slice is explicitly empty on both sides; legacy
            # player rows are still compared by the membership/attribute pairs.
            comparison_legacy_frame = legacy_frame.iloc[0:0]
        # Coach native tables are global/full-history while the legacy table is
        # a requested-season projection. Compare the explicit compatibility
        # slice, not unrelated historical profiles/stints.
        if native.key == 'profiles' and native_frame is not None:
            legacy_ids = set(
                legacy_frame['coach_id'].dropna().astype(str)
                if legacy_frame is not None and 'coach_id' in legacy_frame
                else []
            )
            native_frame = native_frame[
                native_frame['coach_id'].astype(str).isin(legacy_ids)
            ]
        elif native.key == 'stints' and native_frame is not None:
            legacy_keys = set()
            if legacy_frame is not None and not legacy_frame.empty:
                legacy_keys = {
                    (str(row[0]), str(row[1]))
                    for row in legacy_frame[
                        ['current_club_id', 'coach_id']
                    ].dropna().itertuples(index=False, name=None)
                }
            native_frame = native_frame[
                native_frame.apply(
                    lambda row: (
                        str(row.get('club_id')), str(row.get('coach_id')),
                    ) in legacy_keys,
                    axis=1,
                )
            ]
        legacy_rows, legacy_hash = _compatibility_fingerprint(
            comparison_legacy_frame, contract['legacy'],
        )
        native_rows, native_hash = _compatibility_fingerprint(
            native_frame, contract['native'],
        )
        native_batch_id = _frame_batch_id(native_frame, run_key)
        if applicability == 'not_applicable':
            native_batch_id = legacy_batch_id
        status = (
            'success'
            if legacy_batch_id == native_batch_id
            and legacy_rows == native_rows
            and legacy_hash == native_hash
            else 'parity_mismatch'
        )
        if status == 'parity_mismatch':
            # Which keys diverged is the whole diagnosis; a row count and two
            # hashes say only that they did.
            legacy_keys = set(
                _compatibility_keys(comparison_legacy_frame, contract['legacy'])
            )
            native_keys = set(
                _compatibility_keys(native_frame, contract['native'])
            )
            logger.error(
                'PARITY %s: legacy-only=%d native-only=%d; legacy examples=%s; '
                'native examples=%s',
                contract['entity'],
                len(legacy_keys - native_keys),
                len(native_keys - legacy_keys),
                sorted(legacy_keys - native_keys)[:3],
                sorted(native_keys - legacy_keys)[:3],
            )
        sql = f"""
MERGE INTO {DUAL_WRITE_MANIFEST_TABLE} t
USING (VALUES (
    {q(run_key)}, {q(league)}, {int(season)}, {q(contract['entity'])},
    {q(legacy.table_name)},
    {q(native.table_name)}, {q(legacy_batch_id)}, {q(native_batch_id)},
    {legacy_rows}, {native_rows}, {q(legacy_hash)}, {q(native_hash)}, {q(status)}
)) s(cycle_id, league, season, entity, legacy_table, native_table,
     legacy_batch_id, native_batch_id, legacy_rows, native_rows,
     legacy_hash, native_hash, status)
ON t.cycle_id = s.cycle_id AND t.league = s.league
   AND t.season = s.season AND t.entity = s.entity
WHEN MATCHED THEN UPDATE SET
    legacy_table = s.legacy_table, native_table = s.native_table,
    legacy_batch_id = s.legacy_batch_id,
    native_batch_id = s.native_batch_id,
    legacy_rows = s.legacy_rows, native_rows = s.native_rows,
    legacy_hash = s.legacy_hash, native_hash = s.native_hash,
    status = s.status, committed_at = current_timestamp
WHEN NOT MATCHED THEN INSERT (
    cycle_id, league, season, entity, legacy_table, native_table,
    legacy_batch_id, native_batch_id, legacy_rows, native_rows,
    legacy_hash, native_hash, status, committed_at
) VALUES (
    s.cycle_id, s.league, s.season, s.entity, s.legacy_table, s.native_table,
    s.legacy_batch_id, s.native_batch_id, s.legacy_rows, s.native_rows,
    s.legacy_hash, s.native_hash,
    s.status, current_timestamp
)
"""
        _execute_cursor(conn, sql)
        manifest_rows.append({
            'entity': contract['entity'],
            'native_table': native.table_name,
            'legacy_table': legacy.table_name,
            'native_batch_id': native_batch_id,
            'legacy_batch_id': legacy_batch_id,
            'native_rows': native_rows,
            'legacy_rows': legacy_rows,
            'native_hash': native_hash,
            'legacy_hash': legacy_hash,
            'applicability_status': applicability or 'ok',
            'status': status,
        })
    overall = (
        'success'
        if manifest_rows and all(row['status'] == 'success' for row in manifest_rows)
        else 'parity_mismatch'
    )
    return {
        'cycle_id': run_key,
        'league': league,
        'season': int(season),
        'rows': manifest_rows,
        'status': overall,
    }


def _persist_native_write_manifest(
    scraper,
    spec: EntitySpec,
    frames: Mapping[str, Any],
    results: Dict[str, Any],
    run_key: str,
    league: str,
    season: int,
    writer_revision: int,
) -> Dict[str, Any]:
    """Persist exact post-cleanup native-only Bronze business evidence.

    Legacy parity is no longer possible after retention cleanup.  This
    manifest proves the six native entity projections and lets control-plane
    readiness live-rehash their exact batches before promoting an inactive
    Silver/Gold slot.  Empty evidence is promotable only when the typed source
    outcome is explicitly ``authoritative_empty`` or ``not_applicable``.
    """
    revision = int(writer_revision)
    if revision < 0:
        raise ValueError('writer_revision must be non-negative')
    native_specs = [output for output in spec.outputs if not output.is_legacy]
    if not native_specs:
        raise RuntimeError(f'no native write contract for {spec.name}')
    committed_keys = {
        key for key, item in results.get('outputs', {}).items()
        if item.get('table')
    }
    missing = {output.key for output in native_specs} - committed_keys
    if missing:
        raise RuntimeError(
            f'native-only outputs not committed: {sorted(missing)}'
        )

    conn = scraper._bronze_connection()
    _execute_cursor(conn, 'CREATE SCHEMA IF NOT EXISTS iceberg.ops')
    _execute_cursor(
        conn,
        f"CREATE TABLE IF NOT EXISTS {NATIVE_WRITE_MANIFEST_TABLE} ("
        "cycle_id varchar, league varchar, season integer, entity varchar, "
        "native_table varchar, native_batch_id varchar, native_rows bigint, "
        "native_hash varchar, writer_revision bigint, write_mode varchar, "
        "status varchar, committed_at timestamp(6)) WITH (format = 'PARQUET')",
    )

    def q(value: Any) -> str:
        if value is None:
            return 'NULL'
        return "'" + str(value).replace("'", "''") + "'"

    manifest_rows = []
    for native in native_specs:
        frame = frames.get(native.key)
        contract = _MANIFEST_COMPATIBILITY[native.key]
        applicability = _frame_output_summary(frame)['applicability_status']
        native_rows, native_hash = _compatibility_fingerprint(
            frame, contract['native'],
        )
        native_batch_id = _frame_batch_id(frame, run_key)
        allowed_empty = bool(
            results.get('authoritative_empty')
            or applicability in {'authoritative_empty', 'not_applicable'}
        )
        status = (
            'success'
            if native_hash and (native_rows > 0 or allowed_empty)
            else 'empty'
        )
        sql = f"""
MERGE INTO {NATIVE_WRITE_MANIFEST_TABLE} t
USING (VALUES (
    {q(run_key)}, {q(league)}, {int(season)}, {q(contract['entity'])},
    {q(native.table_name)}, {q(native_batch_id)}, {native_rows},
    {q(native_hash)}, {revision}, 'native-only', {q(status)}
)) s(cycle_id, league, season, entity, native_table, native_batch_id,
     native_rows, native_hash, writer_revision, write_mode, status)
ON t.cycle_id = s.cycle_id AND t.league = s.league
   AND t.season = s.season AND t.entity = s.entity
WHEN MATCHED THEN UPDATE SET
    native_table = s.native_table, native_batch_id = s.native_batch_id,
    native_rows = s.native_rows, native_hash = s.native_hash,
    writer_revision = s.writer_revision, write_mode = s.write_mode,
    status = s.status, committed_at = CURRENT_TIMESTAMP
WHEN NOT MATCHED THEN INSERT (
    cycle_id, league, season, entity, native_table, native_batch_id,
    native_rows, native_hash, writer_revision, write_mode, status, committed_at
) VALUES (
    s.cycle_id, s.league, s.season, s.entity, s.native_table,
    s.native_batch_id, s.native_rows, s.native_hash, s.writer_revision,
    s.write_mode, s.status, CURRENT_TIMESTAMP
)
"""
        _execute_cursor(conn, sql)
        manifest_rows.append({
            'entity': contract['entity'],
            'native_table': native.table_name,
            'native_batch_id': native_batch_id,
            'native_rows': native_rows,
            'native_hash': native_hash,
            'writer_revision': revision,
            'write_mode': 'native-only',
            'applicability_status': (
                applicability
                or ('authoritative_empty' if allowed_empty else 'ok')
            ),
            'status': status,
        })
    overall = (
        'success'
        if manifest_rows and all(row['status'] == 'success' for row in manifest_rows)
        else 'incomplete'
    )
    return {
        'cycle_id': run_key,
        'league': league,
        'season': int(season),
        'writer_revision': revision,
        'write_mode': 'native-only',
        'rows': manifest_rows,
        'status': overall,
    }


def _base_result(
    spec: EntitySpec,
    run_key: str,
    refresh_mode: str,
    window_offset: int,
    limit: Optional[int],
) -> Dict[str, Any]:
    return {
        'entity': spec.name,
        'tables': [],
        'outputs': {},
        'rows': 0,
        spec.count_field: 0,
        'fallback': False,
        'fallback_reason': None,
        'errors': [],
        'warnings': [],
        'run_key': run_key,
        'refresh_mode': refresh_mode,
        'window_offset': window_offset,
        'window_limit': limit,
        'native_dual_write': False,
        'write_mode': None,
        'native_write': False,
        'legacy_write': False,
        'native_write_complete': False,
        'native_write_manifest_complete': False,
        'dual_write_complete': False,
        'network_fetches': 0,
        'cache_hits': 0,
        'retries': 0,
        'failures': 0,
        'decoded_response_body_mb': None,
        'wire_mb': None,
        'estimated_wire_response_mb': None,
        'wire_response_bytes': None,
        'provider_up_bytes': None,
        'provider_down_bytes': None,
        'provider_metered_bytes': None,
        'provider_metering_available': False,
        'failed_attempts': 0,
        'budget_status': {},
        'checkpoint_status': 'not_applicable',
        'bootstrap_seeded_keys': 0,
        'traffic': {'telemetry_available': False},
    }


def _run_entity(
    spec: EntitySpec,
    leagues: List[str],
    season: int,
    limit: Optional[int],
    output_path: str,
    dry_run: bool = False,
    force_replace: bool = False,
    window_offset: int = 0,
    refresh_mode: str = 'current',
    run_key: Optional[str] = None,
    native_dual_write: bool = True,
    write_mode: Optional[str] = None,
    expected_reader_revision: Optional[int] = None,
    coach_history_ttl_days: int = 28,
    cycle_budget_bytes: Optional[int] = None,
    cycle_ledger_key: Optional[str] = None,
    retry_budget: Optional[int] = None,
) -> int:
    from scrapers.base.base_scraper import ReplaceGuardError
    from scrapers.transfermarkt import TransfermarktScraper
    from scrapers.transfermarkt.scraper import R0_2B_FALLBACK_MARKER

    if len(leagues) != 1:
        raise ValueError(
            'one runner invocation must receive exactly one mapped competition'
        )
    requested_competition, = leagues
    competition_record = _competition_record(requested_competition)
    league = (
        competition_record.canonical_competition_id
        or f'TM-{competition_record.competition_id}'
    )
    mode = _normalise_write_mode(
        write_mode, native_dual_write=native_dual_write,
    )
    write_spec = _spec_for_write_mode(spec, mode)
    native_write_enabled = mode in {'dual', 'native-only'}
    legacy_write_enabled = mode in {'dual', 'legacy-only'}
    run_key = run_key or _default_run_key(league, season, None, None)
    cycle_ledger_key = cycle_ledger_key or run_key
    results = _base_result(spec, run_key, refresh_mode, window_offset, limit)
    results.update(
        write_mode=mode,
        native_write=native_write_enabled,
        legacy_write=legacy_write_enabled,
        competition_id=competition_record.competition_id,
        edition_id=str(season),
        canonical_season=_canonical_scope_season(
            requested_competition, season,
        ),
        registry_snapshot_id=competition_record.registry_snapshot_id,
        scope_id=os.environ.get('TM_SCOPE_ID'),
        cycle_ledger_key=cycle_ledger_key,
    )
    cycle_budget = None
    exit_code = 1
    scraper = None
    response_cache: Optional[Dict[str, Any]] = None
    cache_path: Optional[str] = None
    selected: Optional[List[str]] = None
    authoritative_frame = None
    checkpoint_frame = None
    checkpoint_spec: Optional[EntitySpec] = None
    coach_memberships = None
    all_coach_memberships = None
    coach_cached_ids: List[str] = []
    coach_cache_frames = None
    career_hydrate_ids: List[str] = []
    career_cache_frames = None
    state_persisted = False
    data_committed = False

    # Keep the configured path even when missing. The scraper owns the
    # fail-closed proxy policy; converting it to None here used to enable the
    # expensive/blocked direct-egress fallback.
    proxy_file = os.environ.get('PROXY_FILE', '/opt/airflow/proxys.txt')

    try:
        if expected_reader_revision is not None:
            # This connection is read-only. It must complete before the shared
            # paid-byte reservation and before TransfermarktScraper constructs
            # its proxy client, so a stale/manual mode cannot spend traffic.
            results['writer_state'] = _authorize_write_mode(
                mode, int(expected_reader_revision),
            )
        if cycle_budget_bytes is not None:
            configured_mb = os.environ.get('TM_DECODED_BODY_BUDGET_MB')
            configured_bytes = min(
                int(float(configured_mb) * MIB)
                if configured_mb is not None
                else int(PRODUCTION_ENTITY_BUDGETS[spec.name]['decoded_mb'] * MIB),
                int(PRODUCTION_ENTITY_BUDGETS[spec.name]['decoded_mb'] * MIB),
            )
            cycle_budget = _prepare_cycle_budget(
                cycle_ledger_key,
                int(cycle_budget_bytes),
                entity=spec.name,
                reserve_bytes=configured_bytes,
            )
            effective_bytes = int(cycle_budget['reserved_bytes'])
            if effective_bytes <= 0:
                raise RuntimeError('no decoded-body budget remains for this entity')
            os.environ['TM_DECODED_BODY_BUDGET_MB'] = str(
                effective_bytes / MIB
            )
            results['cycle_budget'] = dict(cycle_budget)
        response_cache, cache_path, cache_ttl = _load_response_cache()
        with TransfermarktScraper(
            leagues=[league], seasons=[season], proxy_file=proxy_file,
            retry_budget=retry_budget,
            response_cache=response_cache,
            cache_ttl_seconds=cache_ttl,
            canonical_season=os.environ.get('TM_CANONICAL_SEASON'),
        ) as scraper:
            if spec.state_endpoint:
                checkpoint_spec = spec
                (
                    selected, cache_hits, seeded, career_hydrate_ids, coverage,
                ) = _select_player_ids(
                    scraper, spec, league, season, int(limit), window_offset,
                    refresh_mode, run_key, allow_state_writes=not dry_run,
                    legacy_materialization_required=legacy_write_enabled,
                )
                results['cache_hits'] = cache_hits
                results['bootstrap_seeded_keys'] = seeded
                results['roster_coverage'] = coverage
                if mode == 'native-only':
                    # Post-retention native refresh never needs to reconstruct
                    # a deleted legacy season partition from cached global
                    # careers.
                    career_hydrate_ids = []
                if career_hydrate_ids:
                    if not (
                        native_write_enabled
                        and _native_available(scraper, spec.native_reader)
                    ):
                        raise RuntimeError(
                            'career cache projection requires native dual-write'
                        )
                    career_cache_frames = _load_cached_career_frames(
                        scraper, spec, career_hydrate_ids,
                    )
                    results['career_cache_materialized_keys'] = len(
                        career_hydrate_ids
                    )
            elif (
                spec.name == ENTITY_COACHES
                and native_write_enabled
                and _native_available(scraper, spec.native_reader)
            ):
                checkpoint_spec = COACH_HISTORY_CHECKPOINT_SPEC
                all_coach_memberships = _load_coach_memberships(
                    scraper, league, season,
                )
                (
                    coach_memberships,
                    selected,
                    coach_cached_ids,
                    cache_hits,
                    seeded,
                ) = _select_coach_memberships(
                    scraper,
                    all_coach_memberships,
                    league,
                    season,
                    refresh_mode,
                    run_key,
                    coach_history_ttl_days,
                    allow_state_writes=not dry_run,
                )
                results['cache_hits'] = cache_hits
                results['bootstrap_seeded_keys'] = seeded
                results['coach_history_ttl_days'] = coach_history_ttl_days
                results['coach_history_selected_clubs'] = len(selected)
                coach_cache_frames = _load_cached_coach_data(
                    scraper, coach_cached_ids,
                )

            if checkpoint_spec is not None:
                coach_cache_has_rows = bool(
                    coach_cache_frames is not None
                    and not coach_cache_frames['stints'].empty
                )
                career_cache_has_rows = bool(
                    career_cache_frames is not None
                    and any(
                        not frame.empty
                        for frame in career_cache_frames.values()
                    )
                )
                if (
                    selected == []
                    and not coach_cache_has_rows
                    and not career_cache_has_rows
                ):
                    results['skipped'] = True
                    results['skip_reason'] = 'refresh_cache_complete'
                    results['checkpoint_status'] = 'cache_complete'
                    results['native_dual_write'] = bool(
                        mode == 'dual'
                        and _native_available(scraper, spec.native_reader)
                    )
                    # A cache-only retry is successful, but it is not evidence
                    # of a newly committed dual-write cycle for cutover.
                    results['dual_write_complete'] = False
                    exit_code = 0
                    raise _EntityRunComplete(exit_code)

            if (
                spec.name == ENTITY_COACHES
                and selected == []
                and coach_cache_frames is not None
            ):
                frames = _merge_coach_cache_frames(
                    scraper, {}, coach_cache_frames, league, season,
                )
                authoritative_key = (
                    'stints' if mode == 'native-only' else 'legacy_coaches'
                )
                used_native = True
                results['cache_only_materialization'] = True
            elif selected == [] and career_cache_frames is not None:
                frames = _merge_career_cache_frames(
                    scraper, spec, {}, career_cache_frames, league, season,
                )
                authoritative_key = next(iter(career_cache_frames))
                used_native = True
                results['cache_only_materialization'] = True
            else:
                frames, authoritative_key, used_native = _read_frames(
                    scraper, spec, league, season, limit, window_offset, selected,
                    native_dual_write, coach_memberships=coach_memberships,
                    write_mode=mode,
                )
                if spec.name == ENTITY_COACHES and coach_cache_frames is not None:
                    frames = _merge_coach_cache_frames(
                        scraper, frames, coach_cache_frames, league, season,
                    )
                elif career_cache_frames is not None:
                    frames = _merge_career_cache_frames(
                        scraper, spec, frames, career_cache_frames, league, season,
                    )
            allowed_keys = {output.key for output in write_spec.outputs}
            frames = {
                key: frame for key, frame in frames.items()
                if key in allowed_keys
            }
            frames = _align_batch_ids(scraper, frames)
            if spec.name == ENTITY_PLAYERS:
                capture = _native_scope_capture(scraper)
                if capture is not None:
                    results['scope_capture'] = capture
            authoritative_frame = frames.get(authoritative_key)
            checkpoint_frame = (
                frames.get('stints')
                if checkpoint_spec is COACH_HISTORY_CHECKPOINT_SPEC
                else authoritative_frame
            )
            results['native_dual_write'] = bool(used_native and mode == 'dual')

            valid_empty = (
                _valid_empty_ids(scraper, checkpoint_spec, selected)
                if selected is not None and checkpoint_spec is not None else []
            )
            checkpoint_ids = list(dict.fromkeys(
                list(selected or []) + career_hydrate_ids
            ))

            if authoritative_frame is None or getattr(authoritative_frame, 'empty', True):
                if (
                    spec.name == ENTITY_COACHES
                    and selected == []
                    and results.get('cache_only_materialization')
                    and coach_cache_frames is not None
                ):
                    # Cached global stints may legitimately have no overlap
                    # with the requested historical season.  That empty
                    # seasonal projection is authoritative: keep global facts,
                    # clear stale scoped legacy rows, and record empty parity.
                    results['rows'] = 0
                    results[spec.count_field] = 0
                    results['outputs'] = {
                        key: _frame_output_summary(frame)
                        for key, frame in frames.items()
                        if frame is not None and hasattr(frame, '__len__')
                    }
                    for item in results['outputs'].values():
                        if item['rows'] == 0 and not item['applicability_status']:
                            item['applicability_status'] = 'authoritative_empty'
                    if dry_run:
                        results['dry_run'] = True
                        results['dual_write_complete'] = False
                        exit_code = 0
                        raise _EntityRunComplete(exit_code)

                    _save_frames(
                        scraper, write_spec, frames, force_replace, results,
                    )
                    if legacy_write_enabled:
                        scoped_clubs = all_coach_memberships[
                            'club_id'
                        ].dropna().astype(str).tolist()
                        _delete_empty_season_coach_rows(
                            scraper, scoped_clubs, authoritative_frame,
                            league, season,
                        )
                        _delete_removed_coach_clubs(
                            scraper, all_coach_memberships, league, season,
                        )
                        legacy_output = next(
                            output for output in spec.outputs if output.is_legacy
                        )
                        legacy_table = (
                            f'iceberg.bronze.{legacy_output.table_name}'
                        )
                        results['outputs'][legacy_output.key] = {
                            'rows': 0, 'table': legacy_table,
                        }
                        if legacy_table not in results['tables']:
                            results['tables'].append(legacy_table)
                    data_committed = True
                    results['native_write_complete'] = bool(
                        used_native and native_write_enabled
                    )
                    if mode == 'dual':
                        results['batch_manifest'] = _persist_dual_write_manifest(
                            scraper, spec, frames, results, run_key,
                            league, season,
                        )
                        results['dual_write_complete'] = (
                            results['batch_manifest'].get('status') == 'success'
                        )
                        if not results['dual_write_complete']:
                            raise RuntimeError(
                                'empty coach-season compatibility parity failed'
                            )
                    elif mode == 'native-only':
                        results['native_write_manifest'] = (
                            _persist_native_write_manifest(
                                scraper, spec, frames, results, run_key,
                                league, season, int(expected_reader_revision),
                            )
                        )
                        results['native_write_manifest_complete'] = (
                            results['native_write_manifest'].get('status')
                            == 'success'
                        )
                    results['checkpoint_status'] = 'cache_complete'
                    state_persisted = True
                    exit_code = 0
                    raise _EntityRunComplete(exit_code)
                if selected and len(valid_empty) == len(selected):
                    results['valid_empty'] = True
                    results['authoritative_empty'] = True
                    results['rows'] = 0
                    if not dry_run:
                        deleted = _delete_valid_empty_rows(
                            scraper, write_spec, valid_empty, league, season,
                            source_key=checkpoint_spec.id_column,
                        )
                        results['outputs'] = {
                            key: {
                                'rows': 0,
                                'table': table,
                                'applicability_status': 'authoritative_empty',
                            }
                            for key, table in deleted.items()
                        }
                        data_committed = True
                        results['native_write_complete'] = bool(
                            used_native and native_write_enabled
                        )
                        if mode == 'dual' and used_native:
                            results['batch_manifest'] = _persist_dual_write_manifest(
                                scraper, spec, frames, results, run_key,
                                league, season,
                            )
                            results['dual_write_complete'] = (
                                results['batch_manifest'].get('status') == 'success'
                            )
                            if not results['dual_write_complete']:
                                raise RuntimeError(
                                    'valid-empty dual-write compatibility parity failed'
                                )
                        elif mode == 'native-only' and used_native:
                            results['native_write_manifest'] = (
                                _persist_native_write_manifest(
                                    scraper, spec, frames, results, run_key,
                                    league, season,
                                    int(expected_reader_revision),
                                )
                            )
                            results['native_write_manifest_complete'] = (
                                results['native_write_manifest'].get('status')
                                == 'success'
                            )
                        rows = _state_rows(
                            scraper, checkpoint_spec, selected, checkpoint_frame,
                        )
                        checkpoint_status = _commit_checkpoint_or_pending(
                            scraper,
                            checkpoint_spec,
                            selected,
                            rows,
                            run_key,
                            league,
                            season,
                        )
                        results['checkpoint_status'] = checkpoint_status
                        state_persisted = checkpoint_status == 'success'
                        if checkpoint_status == 'committed_checkpoint_pending':
                            results['warnings'].append(
                                'Bronze+manifest committed; ops checkpoint is pending recovery'
                            )
                        elif checkpoint_status != 'success':
                            raise RuntimeError(
                                'valid-empty commit has no recoverable checkpoint'
                            )
                    else:
                        results['dry_run'] = True
                    exit_code = 0
                    raise _EntityRunComplete(exit_code)
                reason = _classify_fallback(scraper)
                logger.error(
                    '%s: %s unavailable — reason=%s',
                    R0_2B_FALLBACK_MARKER, spec.name, reason,
                )
                results['fallback'] = True
                results['fallback_reason'] = reason
                results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {reason}')
                exit_code = _fallback_exit_code(reason)
                raise _EntityRunComplete(exit_code)

            results['rows'] = int(len(authoritative_frame))
            results[spec.count_field] = int(authoritative_frame[spec.id_column].nunique())
            results['outputs'] = {
                key: _frame_output_summary(frame)
                for key, frame in frames.items()
                if frame is not None and hasattr(frame, '__len__')
            }

            scope_dq_required = os.environ.get(
                'TM_SCOPE_DQ_REQUIRED', 'false',
            ).strip().lower() in {'1', 'true', 'yes', 'on'}
            if spec.name == ENTITY_PLAYERS and scope_dq_required:
                current_raw = os.environ.get('TM_EDITION_CURRENT', '').strip().lower()
                if current_raw not in {'true', 'false'}:
                    raise RuntimeError(
                        'TM_EDITION_CURRENT must be explicit for native scope DQ'
                    )
                results['participant_dq'] = _validate_native_player_scope_capture(
                    results.get('scope_capture'),
                    competition_record=competition_record,
                    outputs=results['outputs'],
                    current=current_raw == 'true',
                )

            if dry_run:
                results['dry_run'] = True
                results['dual_write_complete'] = False
                exit_code = 0
                raise _EntityRunComplete(exit_code)

            _save_frames(
                scraper, write_spec, frames, force_replace, results,
            )
            if valid_empty:
                _delete_valid_empty_rows(
                    scraper, write_spec, valid_empty, league, season,
                    source_key=checkpoint_spec.id_column,
                )
            if (
                legacy_write_enabled
                and spec.name == ENTITY_COACHES
                and all_coach_memberships is not None
            ):
                _delete_empty_season_coach_rows(
                    scraper,
                    all_coach_memberships['club_id'].dropna().astype(str).tolist(),
                    frames.get('legacy_coaches'),
                    league,
                    season,
                )
                _delete_removed_coach_clubs(
                    scraper, all_coach_memberships, league, season,
                )
            data_committed = True
            results['native_write_complete'] = bool(
                used_native and native_write_enabled
            )
            if mode == 'dual' and used_native:
                results['batch_manifest'] = _persist_dual_write_manifest(
                    scraper, spec, frames, results, run_key, league, season,
                )
                results['dual_write_complete'] = (
                    results['batch_manifest'].get('status') == 'success'
                )
                if not results['dual_write_complete']:
                    raise RuntimeError(
                        f"dual-write compatibility parity failed for {spec.name}"
                    )
            elif mode == 'native-only' and used_native:
                results['native_write_manifest'] = (
                    _persist_native_write_manifest(
                        scraper, spec, frames, results, run_key,
                        league, season, int(expected_reader_revision),
                    )
                )
                results['native_write_manifest_complete'] = (
                    results['native_write_manifest'].get('status') == 'success'
                )
            if selected is not None and checkpoint_spec is not None:
                rows = _state_rows(
                    scraper, checkpoint_spec, checkpoint_ids, checkpoint_frame,
                )
                checkpoint_status = _commit_checkpoint_or_pending(
                    scraper,
                    checkpoint_spec,
                    checkpoint_ids,
                    rows,
                    run_key,
                    league,
                    season,
                )
                results['checkpoint_status'] = checkpoint_status
                state_persisted = checkpoint_status in {'success', 'cache_complete'}
                if checkpoint_status == 'committed_checkpoint_pending':
                    results['warnings'].append(
                        'Bronze+manifest committed; ops checkpoint is pending recovery'
                    )
                elif checkpoint_status not in {'success', 'cache_complete'}:
                    raise RuntimeError(
                        'Bronze+manifest committed without recoverable checkpoint'
                    )
            exit_code = 0
            raise _EntityRunComplete(exit_code)
    except _EntityRunComplete as complete:
        exit_code = complete.exit_code
    except _EmptyRosterError as exc:
        results['fallback'] = True
        results['fallback_reason'] = 'empty_roster'
        results['errors'].append(f'{R0_2B_FALLBACK_MARKER}: {exc}')
        exit_code = 2
    except ReplaceGuardError as exc:
        message = f'{REPLACE_GUARD_MARKER}: {exc}'
        logger.error(message)
        results['errors'].append(message)
        exit_code = 3
    except Exception as exc:  # noqa: BLE001
        safe_error = _redact_sensitive(exc)
        logger.error(
            '%s scrape failed hard: %s: %s',
            spec.name, type(exc).__name__, safe_error,
        )
        results['errors'].append(safe_error)
        if (
            scraper is not None and selected is not None
            and checkpoint_spec is not None
            and not dry_run and not state_persisted and not data_committed
        ):
            failed = _state_rows(
                scraper,
                checkpoint_spec,
                selected,
                checkpoint_frame,
                failed_error=safe_error,
            )
            _persist_fetch_state(
                scraper, checkpoint_spec, selected, failed, run_key,
            )
        exit_code = 1
    finally:
        # Persist even when the cycle ran out of budget: those pages are paid
        # for, and the next cycle needs them to finish the league.
        _persist_response_cache(response_cache, cache_path)
        if scraper is not None:
            results['traffic'] = _normalise_traffic(scraper)
        traffic = results['traffic']
        results['network_fetches'] = int(traffic.get('network_fetches', 0) or 0)
        results['retries'] = int(traffic.get('retries', 0) or 0)
        results['failures'] = int(traffic.get('failures', 0) or 0)
        results['failed_attempts'] = int(
            traffic.get('failed_attempts', results['failures']) or 0
        )
        results['cache_hits'] += int(traffic.get('cache_hits', 0) or 0)
        results['decoded_response_body_mb'] = traffic.get('decoded_response_body_mb')
        results['decoded_response_body_bytes'] = traffic.get(
            'decoded_response_body_bytes',
        )
        results['wire_mb'] = traffic.get('wire_mb')
        results['wire_response_bytes'] = traffic.get('wire_response_bytes')
        results['provider_up_bytes'] = traffic.get('provider_up_bytes')
        results['provider_down_bytes'] = traffic.get('provider_down_bytes')
        results['provider_metered_bytes'] = traffic.get(
            'provider_metered_bytes'
        )
        results['provider_metering_available'] = bool(
            traffic.get('provider_metering_available', False)
        )
        results['estimated_wire_response_mb'] = traffic.get(
            'estimated_wire_response_mb', results['wire_mb'],
        )
        results['budget_status'] = {
            'exhausted': bool(traffic.get('budget_exhausted', False)),
            'request_attempt_budget': traffic.get('request_attempt_budget'),
            'decoded_body_budget_bytes': traffic.get('decoded_body_budget_bytes'),
        }
        if cycle_budget_bytes is not None and cycle_budget is not None:
            require_metered = os.environ.get(
                'TM_REQUIRE_METERED_PROXY', 'false',
            ).strip().lower() in {'1', 'true', 'yes', 'on'}
            provider_bytes = traffic.get('provider_metered_bytes')
            raw_decoded_bytes = traffic.get('decoded_response_body_bytes')
            authoritative_bytes = (
                provider_bytes if provider_bytes is not None else raw_decoded_bytes
            )
            results['cycle_budget_metric'] = (
                'provider_metered_bytes'
                if provider_bytes is not None else 'decoded_response_body_bytes'
            )
            if require_metered and provider_bytes is None:
                authoritative_bytes = None
                results['errors'].append(
                    'provider-metered traffic unavailable in production lease mode'
                )
            if authoritative_bytes is None:
                results['errors'].append(
                    'raw decoded_response_body_bytes telemetry unavailable and '
                    'no provider-metered replacement exists; refusing to treat '
                    'rounded MiB as paid-traffic evidence'
                )
                # Reserve the entire previously available balance.  The task
                # is red, and even a manually launched later entity with the
                # same cycle id must fail before paid I/O instead of treating
                # unknown traffic as zero.
                try:
                    reserved = max(
                        0,
                        int(
                            cycle_budget.get(
                                'remaining_after_reservation_bytes', 0,
                            ) or 0
                        ),
                    )
                    cycle_after = _record_cycle_traffic(
                        cycle_ledger_key,
                        int(cycle_budget_bytes),
                        f'{spec.name}:telemetry_unknown',
                        reserved,
                    )
                    results['cycle_budget'].update(cycle_after)
                    results['cycle_budget'][
                        'telemetry_unknown_reservation_bytes'
                    ] = reserved + int(
                        cycle_budget.get('reserved_bytes') or 0
                    )
                except Exception as exc:  # noqa: BLE001 - already fail closed
                    results['errors'].append(
                        'cycle traffic ledger fail-closed reservation failed: '
                        f'{_redact_sensitive(exc)}'
                    )
                exit_code = 1
            else:
                try:
                    cycle_after = _record_cycle_traffic(
                        cycle_ledger_key,
                        int(cycle_budget_bytes),
                        spec.name,
                        int(authoritative_bytes),
                        reservation_id=cycle_budget.get('reservation_id'),
                    )
                    results['cycle_budget'].update(cycle_after)
                    if cycle_after['exhausted']:
                        results['errors'].append(
                            'shared cycle decoded-body budget exceeded: '
                            f"{cycle_after['consumed_after_bytes']}/"
                            f"{cycle_after['limit_bytes']} bytes"
                        )
                        exit_code = 1
                except Exception as exc:  # noqa: BLE001 - fail closed for next task
                    safe_error = _redact_sensitive(exc)
                    results['errors'].append(
                        f'cycle traffic ledger persistence failed: {safe_error}'
                    )
                    exit_code = 1
        _write_results(
            output_path,
            results,
            persist_traffic=not dry_run,
        )
    return exit_code


def _parse_output_path(argv: Sequence[str]) -> str:
    try:
        idx = list(argv).index('--output')
        return list(argv)[idx + 1]
    except (ValueError, IndexError):
        return '/tmp/transfermarkt_result.json'


def main() -> int:
    parser = _StrictArgumentParser(description='Run Transfermarkt Bronze scraper')
    parser.add_argument('--entity', default=ENTITY_PLAYERS)
    parser.add_argument('--league', default='ENG-Premier League')
    parser.add_argument(
        '--competition-id', default=None,
        help='Exact Transfermarkt competition id; --league remains a legacy alias.',
    )
    parser.add_argument('--season', type=int, default=2025)
    parser.add_argument(
        '--edition-id', type=int, default=None,
        help='Exact source edition id; overrides the legacy --season alias.',
    )
    parser.add_argument('--limit', type=int, default=None)
    parser.add_argument('--output', default='/tmp/transfermarkt_result.json')
    parser.add_argument('--dry-run', action='store_true')
    parser.add_argument('--force-replace', action='store_true')
    parser.add_argument('--as-of-date', default=None)
    parser.add_argument('--window', type=int, default=None)
    parser.add_argument('--run-key', default=None)
    parser.add_argument(
        '--cycle-ledger-key',
        default=None,
        help=(
            'Shared parent-cycle traffic ledger identity.  The entity run key '
            'remains the exact child-cycle id for writes and checkpoints.'
        ),
    )
    parser.add_argument('--refresh-mode', default='auto', choices=sorted(REFRESH_MODES))
    parser.add_argument(
        '--native-dual-write', action=argparse.BooleanOptionalAction, default=True,
    )
    parser.add_argument(
        '--write-mode',
        choices=sorted(WRITE_MODES),
        default=None,
        help=(
            'Explicit persisted writer lifecycle: dual during retention, '
            'native-only after guarded cleanup, legacy-only for rollback. '
            'Production Airflow derives this from reader state before I/O.'
        ),
    )
    parser.add_argument(
        '--expected-reader-revision',
        type=int,
        default=None,
        help=(
            'Required production CAS pin; runner verifies persisted writer '
            'state before budget reservation and proxy construction.'
        ),
    )
    parser.add_argument('--decoded-body-budget-mb', type=float, default=None)
    parser.add_argument('--request-budget', type=int, default=None)
    parser.add_argument(
        '--retry-budget', type=int, default=None,
        help=(
            'Remaining parent-cycle paid retry budget. The metered proxy '
            'client rejects retry N+1 before network I/O.'
        ),
    )
    parser.add_argument(
        '--cycle-budget-bytes', type=int,
        default=PRODUCTION_CYCLE_BUDGET_BYTES,
    )
    parser.add_argument(
        '--coach-history-ttl-days',
        type=int,
        default=int(os.environ.get('TM_COACH_HISTORY_TTL_DAYS', '28')),
    )

    try:
        args = parser.parse_args()
        entity = args.entity.lower()
        if entity not in VALID_ENTITIES:
            parser.error(f'--entity must be one of {sorted(VALID_ENTITIES)}')
        if args.limit is not None and args.limit <= 0:
            parser.error('--limit must be positive; unlimited/0 is not allowed')
        if args.limit is not None and args.limit > MAX_ROSTER_WINDOW:
            parser.error(f'--limit cannot exceed {MAX_ROSTER_WINDOW}')
        if entity in {ENTITY_MV_HISTORY, ENTITY_TRANSFERS} and args.limit is None:
            parser.error(f'--limit is required for {entity} (max {MAX_ROSTER_WINDOW})')
        if (
            entity in {ENTITY_PLAYERS, ENTITY_COACHES}
            and args.limit is not None
            and not args.dry_run
        ):
            parser.error(f'--limit for {entity} is allowed only with --dry-run')
        if args.window is not None and args.window < 0:
            parser.error('--window must be >= 0')
        if args.decoded_body_budget_mb is not None and args.decoded_body_budget_mb <= 0:
            parser.error('--decoded-body-budget-mb must be positive')
        if args.request_budget is not None and args.request_budget <= 0:
            parser.error('--request-budget must be positive')
        retry_budget_env = os.environ.get('TM_RETRY_BUDGET')
        if retry_budget_env is not None:
            try:
                expected_retry_budget = int(retry_budget_env)
            except ValueError:
                parser.error('TM_RETRY_BUDGET must be a non-negative integer')
            if expected_retry_budget < 0:
                parser.error('TM_RETRY_BUDGET must be a non-negative integer')
            if args.retry_budget is None:
                args.retry_budget = expected_retry_budget
            elif args.retry_budget != expected_retry_budget:
                parser.error('--retry-budget differs from TM_RETRY_BUDGET')
        if args.retry_budget is not None and args.retry_budget < 0:
            parser.error('--retry-budget must be non-negative')
        require_metered = os.environ.get(
            'TM_REQUIRE_METERED_PROXY', 'false',
        ).strip().lower() in {'1', 'true', 'yes', 'on'}
        if require_metered and args.retry_budget is None:
            parser.error('--retry-budget is required for metered proxy runs')
        if args.cycle_budget_bytes is not None and args.cycle_budget_bytes <= 0:
            parser.error('--cycle-budget-bytes must be positive')
        entity_budget = PRODUCTION_ENTITY_BUDGETS[entity]
        if (
            args.decoded_body_budget_mb is not None
            and args.decoded_body_budget_mb > entity_budget['decoded_mb']
        ):
            parser.error(
                f"--decoded-body-budget-mb cannot exceed "
                f"{entity_budget['decoded_mb']} for {entity}"
            )
        if (
            args.request_budget is not None
            and args.request_budget > entity_budget['requests']
        ):
            parser.error(
                f"--request-budget cannot exceed "
                f"{entity_budget['requests']} for {entity}"
            )
        if args.cycle_budget_bytes > PRODUCTION_CYCLE_BUDGET_BYTES:
            parser.error(
                '--cycle-budget-bytes cannot exceed production cap '
                f'{PRODUCTION_CYCLE_BUDGET_BYTES}'
            )
        if args.coach_history_ttl_days <= 0:
            parser.error('--coach-history-ttl-days must be positive')
        if args.expected_reader_revision is None:
            parser.error(
                '--expected-reader-revision is required for every paid writer run'
            )
        if args.expected_reader_revision < 0:
            parser.error('--expected-reader-revision must be non-negative')
    except _ArgparseError as exc:
        safe_error = _redact_sensitive(exc)
        logger.error('Invalid CLI arguments: %s — failing hard', safe_error)
        output = _parse_output_path(sys.argv[1:])
        _write_results(output, {
            'entity': None, 'rows': 0, 'tables': [], 'errors': [safe_error],
            'fallback': False, 'traffic': {'telemetry_available': False},
        })
        return 1

    entity_budget = PRODUCTION_ENTITY_BUDGETS[entity]
    os.environ['TM_DECODED_BODY_BUDGET_MB'] = str(
        args.decoded_body_budget_mb
        if args.decoded_body_budget_mb is not None
        else entity_budget['decoded_mb']
    )
    os.environ['TM_REQUEST_BUDGET'] = str(
        args.request_budget
        if args.request_budget is not None
        else entity_budget['requests']
    )

    competition = args.competition_id or args.league
    edition_id = args.edition_id if args.edition_id is not None else args.season
    try:
        record = _competition_record(competition)
        canonical = _canonical_scope_season(competition, edition_id)
    except Exception as exc:  # classification/season errors are pre-I/O hard failures
        safe_error = _redact_sensitive(exc)
        logger.error('Unsafe Transfermarkt scope: %s', safe_error)
        _write_results(args.output, {
            'entity': entity,
            'rows': 0,
            'tables': [],
            'errors': [safe_error],
            'fallback': False,
            'traffic': {'telemetry_available': False},
        })
        return 1
    from scrapers.transfermarkt.registry import deterministic_scope_id
    scope_id = deterministic_scope_id(record.competition_id, str(edition_id))
    os.environ.setdefault('TM_DAG_ID', os.environ.get(
        'AIRFLOW_CTX_DAG_ID', 'manual_transfermarkt',
    ))
    os.environ.setdefault('TM_RUN_ID', os.environ.get(
        'AIRFLOW_CTX_DAG_RUN_ID', args.run_key or 'manual',
    ))
    os.environ.setdefault('TM_TASK_ID', os.environ.get(
        'AIRFLOW_CTX_TASK_ID', f'scrape_{entity}',
    ))
    os.environ['TM_SCOPE_ID'] = scope_id
    os.environ['TM_COMPETITION_ID'] = record.competition_id
    os.environ['TM_EDITION_ID'] = str(edition_id)
    os.environ['TM_CANONICAL_SEASON'] = canonical

    window_offset = args.window if args.window is not None else _window_offset(args.as_of_date)
    refresh_mode = _resolved_refresh_mode(args.refresh_mode, edition_id)
    run_key = _default_run_key(
        record.competition_id, edition_id, args.as_of_date, args.run_key,
    )
    logger.info(
        'Starting Transfermarkt scraper entity=%s league=%s season=%s limit=%s '
        'window=%s refresh_mode=%s run_key=%s',
        entity, record.competition_id, edition_id, args.limit, window_offset,
        refresh_mode, run_key,
    )
    return _run_entity(
        ENTITY_SPECS[entity], [record.competition_id], edition_id,
        args.limit, args.output,
        dry_run=args.dry_run,
        force_replace=args.force_replace,
        window_offset=window_offset,
        refresh_mode=refresh_mode,
        run_key=run_key,
        native_dual_write=args.native_dual_write,
        write_mode=args.write_mode,
        expected_reader_revision=args.expected_reader_revision,
        coach_history_ttl_days=args.coach_history_ttl_days,
        cycle_budget_bytes=args.cycle_budget_bytes,
        cycle_ledger_key=args.cycle_ledger_key,
        retry_budget=args.retry_budget,
    )


if __name__ == '__main__':
    sys.exit(main())
