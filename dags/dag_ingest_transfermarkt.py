"""Bounded production ingest for Transfermarkt native v2.

The promoted registry is the only source of crawl scopes.  One mapped child
owns one exact ``competition x edition`` cycle; mapped children are serialized
through the paid-proxy pool and share one provider-byte ledger.  The downstream
Silver build is triggered only after every immutable child manifest is green.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
from typing import Any

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import CURRENT_SEASON, DAG_TAGS, SCHEDULES
from utils.default_args import SCRAPER_ARGS

# The budget canon (scrapers/transfermarkt/models.py) is stdlib-only and is
# the single source for every paid-traffic number the DAG pins.
from scrapers.transfermarkt.models import (
    DEFAULT_ENTITY_TIMEOUT_SECONDS,
    MAX_ROSTER_WINDOW,
    MAX_SCOPE_BATCH,
    PARENT_DAILY_HARD_PROVIDER_BYTE_CAP,
    PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP,
    PARENT_REQUEST_LIMIT,
    PARENT_RETRY_LIMIT,
    SCOPE_HARD_PROVIDER_BYTE_CAP,
    SCOPE_REQUEST_LIMIT,
    SCOPE_RETRY_LIMIT,
    SCOPE_SOFT_PROVIDER_BYTE_STOP,
    SCOPE_WALL_CLOCK_TIMEOUT_SECONDS,
)


MV_HISTORY_DAILY_LIMIT = MAX_ROSTER_WINDOW
COACH_HISTORY_TTL_DAYS = 28
CHECKPOINT_TTL_DAYS = 35
PENDING_CHECKPOINT_DIR = '/opt/airflow/logs/transfermarkt-checkpoints'
APPROVAL_JOURNAL = '/opt/airflow/logs/transfermarkt-approvals/journal.json'
# Per-scope caps (one exact competition x edition child cycle).
PROVIDER_HARD_CAP_BYTES = SCOPE_HARD_PROVIDER_BYTE_CAP
PROVIDER_SOFT_STOP_BYTES = SCOPE_SOFT_PROVIDER_BYTE_STOP
PROXY_REQUEST_LIMIT = SCOPE_REQUEST_LIMIT
PROXY_RETRY_LIMIT = SCOPE_RETRY_LIMIT
# Parent (daily) aggregate caps across all mapped scopes of one run.
PARENT_BYTE_BUDGET = PARENT_DAILY_HARD_PROVIDER_BYTE_CAP
PARENT_SOFT_BYTE_STOP = PARENT_DAILY_SOFT_PROVIDER_BYTE_STOP
PROXY_CONCURRENCY = 1
# Only an edition the promoted registry still marks current has to have been
# captured recently; a finished edition's manifest never expires, or a slot that
# takes months to assemble would evict its own history faster than a bounded
# daily crawl could re-earn it.
SCOPE_SET_COVERAGE_MAX_AGE_DAYS = 7
# A partial slot names the scopes it still owes without dragging the whole
# ~9.7k-scope target through XCom.
MAX_REPORTED_SCOPE_IDS = 100
STANDING_POLICY_PATH = (
    '/opt/airflow/dags/configs/transfermarkt/standing_approval_policy.json'
)
STANDING_POLICY_ENV_GATE = 'TM_STANDING_POLICY_ENABLED'

_APPROVAL_FIELDS = (
    'paid_proxy_packet_id',
    'paid_proxy_packet_hash',
    'production_write_packet_id',
    'production_write_packet_hash',
)


def _truthy_env(name: str) -> bool:
    return os.environ.get(name, '').strip().lower() in {
        '1', 'true', 'yes', 'on',
    }


def _preflight_reader_route_for_paid_cycle() -> dict[str, Any]:
    """Pin the live reader revision and inactive slot before any proxy I/O."""

    if not _truthy_env('TM_NATIVE_V2_ENABLED'):
        raise AirflowException(
            'TM_NATIVE_V2_ENABLED must be true before a paid exact cycle'
        )
    from utils import transfermarkt_native_v2 as tm_v2

    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        state = tm_v2.read_reader_state(cur, allow_missing=True)
        if not state.exists:
            raise AirflowException(
                'Transfermarkt control-plane bootstrap is required before '
                'any paid cycle'
            )
        candidate_slot = tm_v2.inactive_slot(state)
        try:
            views = tm_v2.verify_reader_views(
                cur,
                expected_version=state.active_version,
                expected_revision=state.revision,
                expected_slot=(
                    state.active_slot if state.active_version == 'v2' else None
                ),
                allow_static_slot=state.cleanup_completed_at is not None,
            )
        except Exception as exc:  # fresh bootstrap still has physical bases
            views = {'passed': False, 'error': str(exc)}

        if not views['passed']:
            fresh_legacy_bootstrap = bool(
                state.active_version == 'legacy'
                and state.active_slot is None
                and state.revision == 0
            )
            canonical_bases: dict[str, str | None] = {}
            bootstrap_upstreams: dict[str, str | None] = {}
            if fresh_legacy_bootstrap:
                inventory = tm_v2._relation_inventory(cur)
                canonical_bases = {
                    relation.canonical: inventory.get(relation.canonical)
                    for relation in tm_v2.CANONICAL_READER_RELATIONS
                }
                derived_team_value = (
                    'iceberg.gold.transfermarkt_team_season_market_value'
                )
                physical_bases = {
                    name: kind for name, kind in canonical_bases.items()
                    if name != derived_team_value
                }
                bootstrap_upstreams = {
                    'iceberg.silver.xref_team': inventory.get(
                        'iceberg.silver.xref_team'
                    ),
                }
                fresh_legacy_bootstrap = bool(
                    all(
                        kind in {'BASE TABLE', 'TABLE'}
                        for kind in physical_bases.values()
                    )
                    and canonical_bases[derived_team_value]
                    in {None, 'BASE TABLE', 'TABLE'}
                    and all(
                        kind in {'BASE TABLE', 'TABLE', 'VIEW'}
                        for kind in bootstrap_upstreams.values()
                    )
                )
            if not fresh_legacy_bootstrap:
                raise AirflowException(
                    f'Transfermarkt reader preflight failed: {views}'
                )
            views = {
                'passed': True,
                'mode': 'fresh_legacy_base_bootstrap',
                'canonical_bases': canonical_bases,
                'bootstrap_upstreams': bootstrap_upstreams,
            }
    finally:
        cur.close()
        conn.close()

    return {
        'active_version': state.active_version,
        'active_slot': state.active_slot,
        'candidate_slot': candidate_slot,
        'revision': state.revision,
        'reader_views': views,
        'write_mode': (
            'native-only'
            if state.legacy_writers_disabled_at is not None else 'dual'
        ),
        'paid_io_allowed': True,
    }


def _description_name(item: Any) -> str:
    name = getattr(item, 'name', None)
    if name is not None:
        return str(name)
    if isinstance(item, Sequence) and not isinstance(item, (str, bytes)) and item:
        return str(item[0])
    raise AirflowException('promoted-registry query returned invalid metadata')


def _read_promoted_registry(
    *, registry_snapshot_id: str | None = None,
) -> list[dict[str, Any]]:
    """Read the exact promoted registry snapshot; never discover implicitly."""

    from utils import transfermarkt_native_v2 as tm_v2
    from utils.transfermarkt_scope_planner import build_promoted_registry_query

    query = build_promoted_registry_query(
        registry_snapshot_id=registry_snapshot_id or None,
    )
    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        cur.execute(query)
        rows = list(cur.fetchall())
        columns = [_description_name(item) for item in (cur.description or ())]
    finally:
        cur.close()
        conn.close()
    if not rows or not columns:
        raise AirflowException(
            'no exact promoted Transfermarkt registry snapshot is available'
        )
    if any(len(row) != len(columns) for row in rows):
        raise AirflowException('promoted-registry row shape is inconsistent')
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _approval_bundle(
    bundles: Any,
    *,
    scope_id: str,
) -> dict[str, str]:
    if not isinstance(bundles, Mapping):
        raise AirflowException('params.approval_bundles must be an object')
    value = bundles.get(scope_id)
    if not isinstance(value, Mapping):
        raise AirflowException(
            f'{scope_id}: exact paid/write approval bundle is required'
        )
    result = {field: str(value.get(field, '')).strip() for field in _APPROVAL_FIELDS}
    missing = [field for field, item in result.items() if not item]
    if missing:
        raise AirflowException(
            f'{scope_id}: approval bundle is missing {missing}'
        )
    if len(set(result.values())) != len(result):
        raise AirflowException(
            f'{scope_id}: paid and write approvals must be distinct one-shot packets'
        )
    for field in ('paid_proxy_packet_hash', 'production_write_packet_hash'):
        digest = result[field]
        if len(digest) != 64 or any(ch not in '0123456789abcdef' for ch in digest):
            raise AirflowException(f'{scope_id}: {field} must be a sha256 digest')
    return result


def _is_scheduled_run(context: Mapping[str, Any]) -> bool:
    dag_run = context.get('dag_run')
    if dag_run is not None:
        # A present dag_run is the authority: a missing run_type on it means
        # "not scheduled", never a fall-through to the run_id prefix.
        run_type = getattr(dag_run, 'run_type', None)
        return str(getattr(run_type, 'value', run_type)) == 'scheduled'
    return str(context.get('run_id') or '').startswith('scheduled__')


def _plan_exact_scopes(**context: Any) -> list[dict[str, str]]:
    """Build the bounded mapped environments from promoted registry rows."""

    from utils.transfermarkt_scope_planner import plan_transfermarkt_scopes

    ti = context['ti']
    preflight = ti.xcom_pull(task_ids='preflight_reader_route_for_paid_cycle') or {}
    if not preflight.get('paid_io_allowed'):
        raise AirflowException('reader preflight did not allow paid I/O')
    if not os.environ.get('TM_PROXY_CONTROL_URL', '').strip():
        raise AirflowException('TM_PROXY_CONTROL_URL is required; direct fallback is forbidden')

    params = dict(context.get('params') or {})
    journal = Path(str(params.get('approval_journal') or '')).expanduser()
    if not journal.is_absolute():
        raise AirflowException('params.approval_journal must be an absolute path')
    approved_root = Path('/opt/airflow/logs/transfermarkt-approvals')
    if journal != approved_root and approved_root not in journal.parents:
        raise AirflowException('approval journal must be under Airflow logs')

    # A non-empty explicit approval_bundles always wins. The standing policy
    # covers only the scheduler's own runs with empty selectors, behind an env
    # gate: any manual trigger — including one that names scopes/leagues and so
    # skips the planner's dueness filter — keeps the one-shot ritual (the
    # operator has scripts/prepare_transfermarkt_scope_approvals.py for that).
    approval_mode = 'one_shot'
    standing_policy = None
    if (
        not params.get('approval_bundles')
        and not params.get('scopes')
        and not params.get('leagues')
        and _is_scheduled_run(context)
        and _truthy_env(STANDING_POLICY_ENV_GATE)
    ):
        try:
            from dags.scripts.run_transfermarkt_scope_cycle import (
                validate_standing_policy_for_scope_cycle,
            )
        except ModuleNotFoundError:
            from scripts.run_transfermarkt_scope_cycle import (
                validate_standing_policy_for_scope_cycle,
            )
        from utils.transfermarkt_approval import load_standing_policy

        if (
            int(params['proxy_request_limit']) != PROXY_REQUEST_LIMIT
            or int(params['proxy_retry_limit']) != PROXY_RETRY_LIMIT
        ):
            raise AirflowException(
                'standing-policy runs require the pinned '
                f'{PROXY_REQUEST_LIMIT}/{PROXY_RETRY_LIMIT} request/retry limits'
            )
        standing_policy = load_standing_policy(STANDING_POLICY_PATH)
        validate_standing_policy_for_scope_cycle(
            standing_policy,
            write_mode=str(preflight['write_mode']),
            cycle_budget_bytes=PROVIDER_HARD_CAP_BYTES,
            request_limit=PROXY_REQUEST_LIMIT,
            retry_limit=PROXY_RETRY_LIMIT,
        )
        approval_mode = 'standing_policy'

    registry_rows = _read_promoted_registry(
        registry_snapshot_id=str(params.get('registry_snapshot_id') or ''),
    )
    plan = plan_transfermarkt_scopes(
        params,
        parent_cycle_id=str(context['run_id']),
        registry_rows=registry_rows,
        max_batch_size=int(params['max_batch']),
        selection_mode='current_only',
    )
    if not plan.mapped_payloads:
        raise AirflowException('promoted registry produced no exact due scope')

    mapped_envs: list[dict[str, str]] = []
    used_approvals: set[str] = set()
    for payload in plan.mapped_payloads:
        scope_id = str(payload['scope_id'])
        approval = None
        if approval_mode == 'one_shot':
            approval = _approval_bundle(
                params.get('approval_bundles'), scope_id=scope_id,
            )
            identities = set(approval.values())
            overlap = identities & used_approvals
            if overlap:
                raise AirflowException(
                    f'{scope_id}: one-shot approval identity is reused: '
                    f'{sorted(overlap)}'
                )
            used_approvals.update(identities)
        refresh_mode = str(params['refresh_mode'])
        if refresh_mode == 'auto':
            edition_record = payload.get('edition_record') or {}
            if not isinstance(edition_record, Mapping):
                raise AirflowException(f'{scope_id}: edition_record is invalid')
            refresh_mode = (
                'current' if bool(edition_record.get('current')) else 'historical'
            )
        environment = {
            'PYTHONPATH': '/opt/airflow:/opt/airflow/dags',
            'PATH': '/usr/local/bin:/usr/bin:/bin:/home/airflow/.local/bin',
            'HOME': '/home/airflow',
            'TM_REQUIRE_METERED_PROXY': 'true',
            'TM_DAG_ID': str(context['dag'].dag_id),
            'TM_RUN_ID': str(context['run_id']),
            'TM_TASK_ID': 'run_exact_child_cycle',
            'TM_SCOPE_ID': scope_id,
            'TM_SCOPE_PAYLOAD_JSON': json.dumps(
                payload, sort_keys=True, separators=(',', ':'),
            ),
            'TM_READER_REVISION': str(int(preflight['revision'])),
            'TM_CANDIDATE_SLOT': str(preflight['candidate_slot']),
            'TM_WRITE_MODE': str(preflight['write_mode']),
            'TM_APPROVAL_MODE': approval_mode,
            'TM_MV_TRANSFERS_LIMIT': str(int(params['mv_transfers_limit'])),
            'TM_REFRESH_MODE': refresh_mode,
            'TM_COACH_HISTORY_TTL_DAYS': str(
                int(params['coach_history_ttl_days'])
            ),
            'TM_PROXY_LEASE_TTL_SECONDS': str(
                int(params['proxy_lease_ttl_seconds'])
            ),
            'TM_CHECKPOINT_TTL_DAYS': str(
                int(params['checkpoint_ttl_days'])
            ),
            'TM_ENTITY_TIMEOUT_SECONDS': str(
                int(params['entity_timeout_seconds'])
            ),
            'TM_PROVIDER_HARD_CAP_BYTES': str(PROVIDER_HARD_CAP_BYTES),
            'TM_PROVIDER_SOFT_STOP_BYTES': str(PROVIDER_SOFT_STOP_BYTES),
            'TM_PROXY_REQUEST_LIMIT': str(int(params['proxy_request_limit'])),
            'TM_PROXY_RETRY_LIMIT': str(int(params['proxy_retry_limit'])),
            'TM_PARENT_BYTE_BUDGET': str(PARENT_BYTE_BUDGET),
            'TM_PARENT_SOFT_BYTE_STOP': str(PARENT_SOFT_BYTE_STOP),
            'TM_PARENT_REQUEST_LIMIT': str(PARENT_REQUEST_LIMIT),
            'TM_PARENT_RETRY_LIMIT': str(PARENT_RETRY_LIMIT),
            'TM_PENDING_CHECKPOINT_DIR': PENDING_CHECKPOINT_DIR,
        }
        if approval_mode == 'standing_policy':
            environment.update({
                'TM_STANDING_POLICY_PATH': STANDING_POLICY_PATH,
                'TM_STANDING_POLICY_SHA256': standing_policy.policy_hash,
            })
        else:
            environment.update({
                'TM_APPROVAL_JOURNAL': str(journal),
                'TM_PAID_APPROVAL_PACKET_ID': approval['paid_proxy_packet_id'],
                'TM_PAID_APPROVAL_PACKET_HASH': approval[
                    'paid_proxy_packet_hash'
                ],
                'TM_WRITE_APPROVAL_PACKET_ID': approval[
                    'production_write_packet_id'
                ],
                'TM_WRITE_APPROVAL_PACKET_HASH': approval[
                    'production_write_packet_hash'
                ],
            })
        mapped_envs.append(environment)
    return mapped_envs


def _load_json_object(path: str, *, label: str) -> dict[str, Any]:
    file_path = Path(path)
    if not file_path.is_absolute() or not file_path.is_file():
        raise AirflowException(f'{label} is missing: {path}')
    try:
        value = json.loads(file_path.read_text(encoding='utf-8'))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException(f'{label} is unreadable: {path}') from exc
    if not isinstance(value, dict):
        raise AirflowException(f'{label} must contain a JSON object: {path}')
    return value


def _sql_literal(value: str) -> str:
    return "'" + str(value).replace("'", "''") + "'"


def _read_completed_scope_manifest_rows(
    *, reader_revision: int,
) -> list[dict[str, Any]]:
    """Read every exact child manifest accumulated by earlier bounded batches.

    The slot is cumulative: a bounded batch buys at most eight of the ~9.7k
    target scopes, so an inactive slot can only ever be assembled by adding this
    batch to everything earlier batches already proved.

    Two things are therefore deliberately NOT filtered here.  Age: a settled
    edition's manifest is evidence forever, and evicting it after a week would
    destroy accumulated coverage faster than a bounded daily crawl could re-earn
    it.  Registry snapshot: the snapshot id is a hash over every page of the
    source registry, so any byte that moves on the site mints a new one and
    discovery runs monthly — filtering on it emptied the pool at every rotation
    and made the target unreachable in principle.  What a manifest captured does
    not change when the registry is re-read; whether the registry still means
    the same thing by that scope is checked by the caller, scope by scope.
    """

    from utils import transfermarkt_native_v2 as tm_v2
    from utils.transfermarkt_scope_state import (
        SCOPE_COMPLETION_STATUS,
        SCOPE_MANIFEST_TABLE,
    )

    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        cur.execute(f"""
SELECT parent_cycle_id, child_cycle_id, scope_id, competition_id, edition_id,
       canonical_competition_id, canonical_season, registry_snapshot_id,
       capture_revision, parser_revision, schema_revision, reader_revision,
       entity_manifest_json, manifest_digest, status, committed_at, is_fresh
FROM (
    SELECT *,
           committed_at >= CURRENT_TIMESTAMP
               - INTERVAL '{SCOPE_SET_COVERAGE_MAX_AGE_DAYS}' DAY AS is_fresh,
           ROW_NUMBER() OVER (
               PARTITION BY scope_id
               ORDER BY committed_at DESC, reader_revision DESC
           ) AS rn
    FROM {SCOPE_MANIFEST_TABLE}
    WHERE status = {_sql_literal(SCOPE_COMPLETION_STATUS)}
      AND reader_revision <= {int(reader_revision)}
)
WHERE rn = 1
ORDER BY scope_id
""")
        rows = list(cur.fetchall())
        columns = [_description_name(item) for item in (cur.description or ())]
    finally:
        cur.close()
        conn.close()
    if rows and (not columns or any(len(row) != len(columns) for row in rows)):
        raise AirflowException('persisted scope-manifest row shape is inconsistent')
    return [dict(zip(columns, row, strict=True)) for row in rows]


def _manifest_from_ops_row(value: Mapping[str, Any]):
    from utils import transfermarkt_native_v2 as tm_v2
    from utils.transfermarkt_scope_state import (
        SCOPE_COMPLETION_STATUS,
        ScopeManifest,
        ScopeManifestError,
    )

    try:
        entity_value = value['entity_manifest_json']
        if isinstance(entity_value, str):
            entity_value = json.loads(entity_value)
        if not isinstance(entity_value, Mapping) or set(entity_value) != {
            'entities', 'dq_evidence',
        }:
            raise ScopeManifestError(
                'persisted entity manifest must contain entities and dq_evidence'
            )
        manifest = ScopeManifest.from_mapping({
            key: value[key]
            for key in (
                'parent_cycle_id', 'child_cycle_id', 'scope_id',
                'competition_id', 'edition_id', 'canonical_competition_id',
                'canonical_season', 'registry_snapshot_id', 'capture_revision',
                'parser_revision', 'schema_revision', 'reader_revision',
            )
        } | {
            'entities': entity_value['entities'],
            'dq_evidence': entity_value['dq_evidence'],
        })
        manifest.validate(tm_v2.NATIVE_ENTITIES)
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise ScopeManifestError('persisted scope manifest is invalid') from exc
    if str(value.get('status')) != SCOPE_COMPLETION_STATUS:
        raise ScopeManifestError(f'{manifest.scope_id}: scope is not complete')
    if manifest.digest != str(value.get('manifest_digest') or ''):
        raise ScopeManifestError(
            f'{manifest.scope_id}: persisted manifest digest mismatch'
        )
    return manifest


def _registry_meaning_drifted(manifest, target: Mapping[str, Any]) -> bool:
    """Does the promoted registry still mean by this scope what was captured?

    Only the attributes that change the meaning of the captured rows are
    compared: the canonical identity the scope is published under, and the
    classification that decided which entities were applicable to it.  A new
    registry snapshot that merely re-read the same competition changes none of
    them, and the manifest stays valid evidence.
    """

    capture = manifest.dq_evidence['scope_capture']
    stated = {
        'canonical_competition_id': str(manifest.canonical_competition_id),
        'canonical_season': str(manifest.canonical_season),
        'competition_type': str(capture['competition_type']),
        'team_type': str(capture['team_type']),
        'gender': str(capture['gender']),
        'age_category': str(capture['age_category']),
    }
    for field, captured in stated.items():
        expected = target.get(field)
        if expected in (None, ''):
            continue
        if str(expected) != captured:
            return True
    return False


def _build_scope_set(
    planned_envs: Sequence[Mapping[str, str]],
    preflight: Mapping[str, Any],
    *,
    target_scopes: Sequence[Mapping[str, Any]] | None = None,
    persisted_manifest_rows: Sequence[Mapping[str, Any]] = (),
) -> dict[str, Any]:
    """Validate exact immutable manifests and the shared provider ledger."""

    from utils import transfermarkt_native_v2 as tm_v2
    from utils.transfermarkt_scope_state import (
        CAPTURE_REVISION,
        ScopeManifest,
        ScopeManifestError,
        ScopeSetManifest,
        aggregate_traffic,
    )

    if not planned_envs:
        raise AirflowException('scope-set gate received no mapped scopes')
    manifests = []
    payloads = []
    ledger_paths: set[str] = set()
    try:
        for environment in planned_envs:
            payload = json.loads(environment['TM_SCOPE_PAYLOAD_JSON'])
            manifest_value = _load_json_object(
                payload['result_paths']['scope_manifest'],
                label=f"{payload['scope_id']} scope manifest",
            )
            manifest = ScopeManifest.from_mapping(manifest_value)
            manifest.validate(tm_v2.NATIVE_ENTITIES)
            if manifest_value.get('manifest_digest') != manifest.digest:
                raise AirflowException(
                    f"{payload['scope_id']}: immutable manifest digest mismatch"
                )
            # A scope crawled under the standing policy must carry that exact
            # policy inside its manifest hash: the schedule has no one-shot
            # packet and no journal, so this is the only authorization trace it
            # leaves, and an unsigned manifest would make the trace optional.
            policy_hash = environment.get('TM_STANDING_POLICY_SHA256')
            stated_policy = manifest.dq_evidence.get('standing_policy_hash')
            if environment.get('TM_APPROVAL_MODE') == 'standing_policy':
                if stated_policy != policy_hash:
                    raise AirflowException(
                        f"{payload['scope_id']}: manifest does not carry the "
                        'standing policy that authorized it'
                    )
            elif stated_policy is not None:
                raise AirflowException(
                    f"{payload['scope_id']}: one-shot scope claims a standing "
                    'policy authorization'
                )
            expected_identity = {
                'parent_cycle_id': payload['parent_cycle_id'],
                'child_cycle_id': payload['child_cycle_id'],
                'scope_id': payload['scope_id'],
                'competition_id': payload['competition_id'],
                'edition_id': payload['edition_id'],
                'canonical_competition_id': payload[
                    'canonical_competition_id'
                ],
                'canonical_season': payload['canonical_season'],
                'registry_snapshot_id': payload['registry_snapshot_id'],
                'reader_revision': int(preflight['revision']),
            }
            actual_identity = {
                key: getattr(manifest, key) for key in expected_identity
            }
            if actual_identity != expected_identity:
                raise AirflowException(
                    f"{payload['scope_id']}: scope manifest identity drift"
                )
            manifests.append(manifest)
            payloads.append(payload)
            ledger_paths.add(str(payload['parent_ledger']['path']))
    except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
        raise AirflowException('mapped scope payload is invalid') from exc
    except ScopeManifestError as exc:
        raise AirflowException(f'scope manifest failed closed: {exc}') from exc

    if len(ledger_paths) != 1:
        raise AirflowException('mapped scopes do not share one parent proxy ledger')
    traffic = aggregate_traffic(manifests)
    if traffic['provider_metered_bytes'] > PARENT_BYTE_BUDGET:
        raise AirflowException(
            'parent daily provider byte budget exceeded: '
            f"{traffic['provider_metered_bytes']}/{PARENT_BYTE_BUDGET}"
        )
    ledger = _load_json_object(next(iter(ledger_paths)), label='parent proxy ledger')
    required_ledger = {
        'provider_metered_bytes': traffic['provider_metered_bytes'],
        'requests': traffic['requests'],
        'retries': traffic['retries'],
        'hard_provider_byte_budget': PARENT_BYTE_BUDGET,
        'soft_provider_byte_stop': PARENT_SOFT_BYTE_STOP,
    }
    if any(int(ledger.get(key, -1)) != value for key, value in required_ledger.items()):
        raise AirflowException('parent proxy ledger disagrees with scope manifests')

    current_manifests = tuple(manifests)
    targets = {
        str(item['scope_id']): item
        for item in (dict(value) for value in target_scopes or ())
    }
    if not targets:
        targets = {
            item.scope_id: {'scope_id': item.scope_id, 'current': False}
            for item in current_manifests
        }
    if len(targets) != len(tuple(target_scopes or targets)):
        raise AirflowException('registry slot target contains empty/duplicate scope ids')
    target_ids = set(targets)
    current_identity = {
        (
            item.capture_revision, item.parser_revision, item.schema_revision,
        )
        for item in current_manifests
    }
    if len(current_identity) != 1:
        raise AirflowException('current bounded batch has mixed capture revisions')
    capture, parser, schema = next(iter(current_identity))
    # The capture revision is the contract every member of a slot must share.
    # Pin it to the canon: it once defaulted to a per-batch selection hash, and
    # the two manifests that were captured under that default can never join a
    # slot again.  A drift here must fail the cycle, not orphan its evidence.
    if capture != CAPTURE_REVISION:
        raise AirflowException(
            f'bounded batch states capture revision {capture!r}; the canon is '
            f'{CAPTURE_REVISION!r} and a slot may only hold one contract'
        )
    batch_snapshots = {item.registry_snapshot_id for item in current_manifests}
    if len(batch_snapshots) != 1:
        raise AirflowException(
            'bounded batch spans registry snapshots: '
            f'{sorted(batch_snapshots)}'
        )
    registry = next(iter(batch_snapshots))
    by_scope = {item.scope_id: item for item in current_manifests}
    stale: list[str] = []
    drifted: list[str] = []
    incompatible: list[str] = []
    retired: list[str] = []
    for row in persisted_manifest_rows:
        try:
            candidate = _manifest_from_ops_row(row)
        except ScopeManifestError as exc:
            raise AirflowException(f'persisted scope manifest failed closed: {exc}') from exc
        if candidate.scope_id in by_scope:
            continue
        if candidate.scope_id not in target_ids:
            # The promoted registry no longer targets this scope at all: the
            # competition was retired, or the edition deactivated.  Dropping it
            # from the slot is correct — but it is a coverage change and it is
            # named, not silent.  (A scope the registry still targets but now
            # describes differently is a DIFFERENT case; see below.)
            retired.append(candidate.scope_id)
            continue
        # The manifest's registry snapshot is NOT compared: a snapshot id is a
        # hash over the whole source registry, so it changes whenever anything
        # anywhere on the site moves, and requiring the slot's members to share
        # one wiped every earlier batch at each discovery run.  What must still
        # hold is that the promoted registry still means the same thing by this
        # scope — same canonical identity, same classification.  If it does not,
        # what was captured no longer describes what the registry now targets,
        # and the scope has to be crawled again.
        if (
            candidate.capture_revision,
            candidate.parser_revision,
            candidate.schema_revision,
        ) != (capture, parser, schema):
            incompatible.append(candidate.scope_id)
            continue
        if _registry_meaning_drifted(candidate, targets[candidate.scope_id]):
            drifted.append(candidate.scope_id)
            continue
        if int(candidate.reader_revision) > int(preflight['revision']):
            raise AirflowException(
                f'{candidate.scope_id}: persisted reader revision is from the future'
            )
        # A stale current edition stays IN the slot: dropping it would shrink
        # the slot, and a slot that can shrink can never pass the cutover
        # monotonicity gate again.  Its age is carried into readiness, where it
        # gates the reader flip instead.
        if (
            bool(targets[candidate.scope_id].get('current'))
            and not bool(row.get('is_fresh', True))
        ):
            stale.append(candidate.scope_id)
        by_scope[candidate.scope_id] = candidate
    unexpected = sorted(set(by_scope) - target_ids)
    if unexpected:
        raise AirflowException(
            f'bounded batch contains scopes outside promoted slot target: {unexpected}'
        )
    missing = sorted(target_ids - set(by_scope))
    complete_manifests = tuple(by_scope[key] for key in sorted(by_scope))
    # The slot is promoted with whatever it has proved so far; how much of the
    # target that is, is reported, not gated.  Demanding the whole target here
    # was unsatisfiable by construction: at eight scopes per bounded daily batch
    # the 9.7k-scope target takes months, and a 7-day eviction of the evidence
    # made the collected part expire faster than the rest could be bought.
    scope_set = ScopeSetManifest.build(
        complete_manifests,
        expected_entities=tm_v2.NATIVE_ENTITIES,
        reader_revision=int(preflight['revision']),
        # The slot is bound to the promoted snapshot this batch was planned
        # against; its members may predate it.
        registry_snapshot_id=registry,
    )
    parent_ids = {str(payload['parent_cycle_id']) for payload in payloads}
    if len(parent_ids) != 1:
        raise AirflowException('scope set spans multiple parent cycles')
    continuation_required = any(
        bool(payload.get('continuation_required')) for payload in payloads
    )
    remaining_count = max(
        (int(payload.get('remaining_count', 0)) for payload in payloads),
        default=0,
    )
    return {
        'parent_cycle_id': next(iter(parent_ids)),
        'scope_set_id': scope_set.scope_set_id,
        'scope_set_manifest': scope_set.as_dict(),
        'scope_count': len(complete_manifests),
        'coverage_target_count': len(target_ids),
        'coverage_complete': not missing,
        'coverage_ratio': len(complete_manifests) / len(target_ids),
        'missing_scope_count': len(missing),
        'missing_scope_ids': missing[:MAX_REPORTED_SCOPE_IDS],
        'registry_snapshot_id': registry,
        'stale_current_scope_count': len(stale),
        'stale_current_scope_ids': sorted(stale)[:MAX_REPORTED_SCOPE_IDS],
        'registry_drift_scope_count': len(drifted),
        'registry_drift_scope_ids': sorted(drifted)[:MAX_REPORTED_SCOPE_IDS],
        'retired_scope_count': len(retired),
        'retired_scope_ids': sorted(retired)[:MAX_REPORTED_SCOPE_IDS],
        'incompatible_capture_scope_count': len(incompatible),
        'incompatible_capture_scope_ids': (
            sorted(incompatible)[:MAX_REPORTED_SCOPE_IDS]
        ),
        'traffic': aggregate_traffic(complete_manifests),
        'current_batch_traffic': traffic,
        'candidate_slot': str(preflight['candidate_slot']),
        'reader_revision': int(preflight['revision']),
        'continuation_required': continuation_required or bool(missing),
        'remaining_count': max(remaining_count, len(missing)),
    }


def _validate_scope_set(**context: Any) -> dict[str, Any]:
    ti = context['ti']
    planned_envs = ti.xcom_pull(task_ids='plan_exact_scopes') or []
    preflight = (
        ti.xcom_pull(task_ids='preflight_reader_route_for_paid_cycle') or {}
    )
    from utils.transfermarkt_scope_planner import eligible_registry_scopes

    params = dict(context.get('params') or {})
    registry_rows = _read_promoted_registry(
        registry_snapshot_id=str(params.get('registry_snapshot_id') or ''),
    )
    targets = eligible_registry_scopes(registry_rows)
    snapshot_ids = {
        str(row.get('registry_snapshot_id') or '') for row in registry_rows
    }
    if len(snapshot_ids) != 1 or '' in snapshot_ids:
        raise AirflowException('promoted registry rows do not share one snapshot')
    persisted_rows = _read_completed_scope_manifest_rows(
        reader_revision=int(preflight['revision']),
    )
    result = _build_scope_set(
        planned_envs,
        preflight,
        target_scopes=[item.as_dict() for item in targets],
        persisted_manifest_rows=persisted_rows,
    )
    # The slot is frozen against the snapshot this paid batch was planned under.
    if result['registry_snapshot_id'] != next(iter(snapshot_ids)):
        raise AirflowException(
            'bounded batch was captured under another registry snapshot than '
            'the promoted one'
        )

    # No reader transition may race the paid cycle or its Silver build.
    from utils import transfermarkt_native_v2 as tm_v2

    conn = tm_v2.connect()
    cur = conn.cursor()
    try:
        state = tm_v2.read_reader_state(cur, allow_missing=False)
    finally:
        cur.close()
        conn.close()
    if (
        state.revision != result['reader_revision']
        or tm_v2.inactive_slot(state) != result['candidate_slot']
    ):
        raise AirflowException('reader revision/candidate slot changed during crawl')
    explicit_scope_selection = bool(
        params.get('scopes') or params.get('leagues')
    )
    result['explicit_scope_selection'] = explicit_scope_selection
    # The cumulative slot is always promotable to the INACTIVE slot: it is the
    # sum of every scope this crawl has ever proved under the promoted registry
    # snapshot, and every bounded batch only adds to it.  What the slot still
    # owes — the uncaptured share of the target, and the career-fact debt inside
    # the captured part — is carried into the readiness report and blocks the
    # reader cutover there, not the build here.
    result['promotion_ready'] = True
    result['transform_conf'] = {
        'transfermarkt_parent_cycle_id': result['parent_cycle_id'],
        'transfermarkt_scope_set_id': result['scope_set_id'],
        'transfermarkt_scope_set_manifest': result['scope_set_manifest'],
        'transfermarkt_reader_revision': result['reader_revision'],
        'transfermarkt_candidate_slot': result['candidate_slot'],
    }
    if result['coverage_complete']:
        result['next_action'] = (
            'issue fresh exact Silver and Gold production-write packets, then '
            'trigger dag_transform_transfermarkt_silver with transform_conf '
            'plus the packet refs and approval journal'
        )
    else:
        result['next_action'] = (
            f"promote this cumulative slot ({result['scope_count']}/"
            f"{result['coverage_target_count']} target scopes) through "
            'dag_transform_transfermarkt_silver; keep capturing the remaining '
            'scopes — the reader cutover stays blocked by the readiness gates '
            'until the slot covers the legacy branch and its career debt falls '
            'under the ceiling'
        )
    return result


with DAG(
    dag_id='dag_ingest_transfermarkt',
    default_args=SCRAPER_ARGS,
    description='Bounded registry-driven Transfermarkt native-v2 ingest',
    schedule=SCHEDULES.get('dag_ingest_transfermarkt', '0 4 * * *'),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=DAG_TAGS.get(
        'transfermarkt', ['scraping', 'transfermarkt', 'bronze', 'football'],
    ),
    max_active_runs=1,
    params={
        'scopes': Param(default=[], type='array'),
        'leagues': Param(default=[], type='array'),
        'season': Param(
            default=None,
            type=['null', 'integer'],
            minimum=1900,
            maximum=CURRENT_SEASON + 1,
        ),
        'registry_snapshot_id': Param(default='', type='string'),
        'max_batch': Param(
            default=MAX_SCOPE_BATCH, type='integer', minimum=1,
            maximum=MAX_SCOPE_BATCH,
        ),
        'approval_journal': Param(default=APPROVAL_JOURNAL, type='string'),
        'approval_bundles': Param(default={}, type='object'),
        'mv_transfers_limit': Param(
            default=MV_HISTORY_DAILY_LIMIT,
            type='integer', minimum=1, maximum=MV_HISTORY_DAILY_LIMIT,
        ),
        'refresh_mode': Param(
            default='auto', type='string',
            enum=['auto', 'current', 'historical', 'force'],
        ),
        'coach_history_ttl_days': Param(
            default=COACH_HISTORY_TTL_DAYS,
            type='integer', minimum=1, maximum=90,
        ),
        'checkpoint_ttl_days': Param(
            default=CHECKPOINT_TTL_DAYS,
            type='integer', minimum=1, maximum=90,
        ),
        'proxy_lease_ttl_seconds': Param(
            default=3600, type='integer', minimum=60, maximum=3600,
        ),
        'proxy_request_limit': Param(
            default=PROXY_REQUEST_LIMIT,
            type='integer', minimum=1, maximum=PROXY_REQUEST_LIMIT,
        ),
        'proxy_retry_limit': Param(
            default=PROXY_RETRY_LIMIT,
            type='integer', minimum=0, maximum=PROXY_RETRY_LIMIT,
        ),
        'entity_timeout_seconds': Param(
            default=DEFAULT_ENTITY_TIMEOUT_SECONDS, type='integer',
            minimum=60, maximum=DEFAULT_ENTITY_TIMEOUT_SECONDS,
        ),
    },
    doc_md="""
    The scheduled run reads only the promoted central competition registry.
    Empty selectors mean all due eligible senior-men scopes, capped at eight.
    A non-empty params.approval_bundles keeps the manual ritual: every mapped
    child needs separate one-shot paid-proxy and production-write approvals.
    Only a run_type=scheduled run with empty bundles and empty scopes/leagues
    selectors, and TM_STANDING_POLICY_ENABLED=true, is instead covered by the
    committed standing approval policy
    (dags/configs/transfermarkt/standing_approval_policy.json), whose caps must
    equal the pinned wrapper limits and whose sha256 is re-verified by each
    child. Any manual trigger — with or without explicit selectors — and any
    run without the gate still fails closed without exact one-shot bundles
    (scripts/prepare_transfermarkt_scope_approvals.py issues them). Unknown
    classification, missing approval, missing manifest, unknown provider
    traffic, reader drift, or DQ failure blocks scope-set completion.
    Silver/Gold are a second Airflow phase because their exact
    one-shot approvals can only be issued after this DAG computes scope_set_id.
    Scheduled empty selectors are capture-only; only an explicit bounded
    scopes/leagues run can become promotion-ready.
    """,
) as dag:
    preflight_reader_route_task = PythonOperator(
        task_id='preflight_reader_route_for_paid_cycle',
        python_callable=_preflight_reader_route_for_paid_cycle,
    )

    plan_exact_scopes_task = PythonOperator(
        task_id='plan_exact_scopes',
        python_callable=_plan_exact_scopes,
    )

    run_exact_child_cycle_task = BashOperator.partial(
        task_id='run_exact_child_cycle',
        bash_command=r'''set -euo pipefail
cd /opt/airflow
case "$TM_APPROVAL_MODE" in
  standing_policy)
    approval_args=(
      --standing-policy "$TM_STANDING_POLICY_PATH"
      --standing-policy-sha256 "$TM_STANDING_POLICY_SHA256"
    )
    ;;
  one_shot)
    approval_args=(
      --approval-journal "$TM_APPROVAL_JOURNAL"
      --paid-proxy-approval-packet-id "$TM_PAID_APPROVAL_PACKET_ID"
      --paid-proxy-approval-packet-hash "$TM_PAID_APPROVAL_PACKET_HASH"
      --production-write-approval-packet-id "$TM_WRITE_APPROVAL_PACKET_ID"
      --production-write-approval-packet-hash "$TM_WRITE_APPROVAL_PACKET_HASH"
    )
    ;;
  *)
    echo "unknown TM_APPROVAL_MODE: $TM_APPROVAL_MODE" >&2
    exit 1
    ;;
esac
exec python dags/scripts/run_transfermarkt_scope_cycle.py \
  --payload-json "$TM_SCOPE_PAYLOAD_JSON" \
  --reader-revision "$TM_READER_REVISION" \
  --candidate-slot "$TM_CANDIDATE_SLOT" \
  --write-mode "$TM_WRITE_MODE" \
  "${approval_args[@]}" \
  --career-window-limit "$TM_MV_TRANSFERS_LIMIT" \
  --refresh-mode "$TM_REFRESH_MODE" \
  --coach-history-ttl-days "$TM_COACH_HISTORY_TTL_DAYS" \
  --checkpoint-ttl-days "$TM_CHECKPOINT_TTL_DAYS" \
  --lease-ttl-seconds "$TM_PROXY_LEASE_TTL_SECONDS" \
  --entity-timeout-seconds "$TM_ENTITY_TIMEOUT_SECONDS" \
  --cycle-budget-bytes "$TM_PROVIDER_HARD_CAP_BYTES" \
  --soft-byte-stop-bytes "$TM_PROVIDER_SOFT_STOP_BYTES" \
  --request-limit "$TM_PROXY_REQUEST_LIMIT" \
  --retry-limit "$TM_PROXY_RETRY_LIMIT" \
  --parent-byte-budget "$TM_PARENT_BYTE_BUDGET" \
  --parent-soft-byte-stop "$TM_PARENT_SOFT_BYTE_STOP" \
  --parent-request-limit "$TM_PARENT_REQUEST_LIMIT" \
  --parent-retry-limit "$TM_PARENT_RETRY_LIMIT"''',
        append_env=True,
        retries=0,
        pool='transfermarkt_proxy',
        pool_slots=1,
        # The proxy permit scheduler also prioritises this traffic class, but
        # Airflow should prefer the daily writer before either task reaches it.
        priority_weight=100,
        max_active_tis_per_dag=1,
        # Derived from the budget canon, never a literal: the task supervises
        # four entity subprocesses whose own timeouts sum to 18000 s (the
        # career entities need 5400 s each at a full 650-attempt budget), plus
        # the scope's ops MERGEs.  A shorter task timeout would SIGKILL a
        # runner mid-crawl and lose its attempt-guard write and evidence.
        # Scopes run strictly serially (max_active_tis_per_dag=1) and runs do
        # not overlap or backfill (max_active_runs=1, catchup=False), so a
        # long worst-case DagRun only delays the next scheduled one.
        execution_timeout=timedelta(seconds=SCOPE_WALL_CLOCK_TIMEOUT_SECONDS),
        do_xcom_push=False,
    ).expand(env=plan_exact_scopes_task.output)

    validate_scope_set_task = PythonOperator(
        task_id='validate_scope_set',
        python_callable=_validate_scope_set,
    )

    (
        preflight_reader_route_task
        >> plan_exact_scopes_task
        >> run_exact_child_cycle_task
        >> validate_scope_set_task
    )
