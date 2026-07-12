"""Monthly, approval-gated Transfermarkt competition registry discovery.

The paid discovery and its two Bronze writes run in one proxy-only process.
Silver publication is a separate boundary: the exact discovery manifest is
validated and rendered into a side-effect-free publication plan first, then a
third one-shot approval is consumed immediately before the Trino connection is
opened.  An unknown or conflicting active classification therefore cannot
reach Silver or advance the canonical registry CAS pointer.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from datetime import datetime, timedelta
import hashlib
import json
import os
from pathlib import Path
import tempfile
from typing import Any
from urllib.parse import urlsplit

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models.param import Param
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

from utils.config import DAG_TAGS
from utils.default_args import SCRAPER_ARGS


DAG_ID = "dag_discover_transfermarkt_registry"
DISCOVERY_TASK_ID = "discover_registry"
DISCOVERY_SCRIPT = "/opt/airflow/dags/scripts/run_transfermarkt_discovery.py"
STATE_ROOT = Path("/opt/airflow/logs/transfermarkt-registry")
OUTPUT_ROOT = STATE_ROOT / "manifests"
CACHE_PATH = STATE_ROOT / "cache" / "http.json"
APPROVAL_ROOT = Path("/opt/airflow/logs/transfermarkt-approvals")
APPROVAL_JOURNAL = APPROVAL_ROOT / "journal.json"

PROVIDER_HARD_CAP_BYTES = 15 * 1024 * 1024
PROVIDER_HARD_CAP_MIB = 15
PROXY_REQUEST_LIMIT = 1024
PROXY_RETRY_LIMIT = 96
PROXY_CONCURRENCY = 1
CACHE_TTL_SECONDS = 24 * 60 * 60
LEASE_TTL_SECONDS = 60 * 60

BRONZE_TABLES = (
    "iceberg.bronze.transfermarkt_competitions",
    "iceberg.bronze.transfermarkt_competition_editions",
)
_DIGEST = frozenset("0123456789abcdef")


def _digest(value: str, *, field: str) -> str:
    result = str(value or "").strip()
    if len(result) != 64 or any(character not in _DIGEST for character in result):
        raise AirflowException(f"{field} must be a sha256 digest")
    return result


def _absolute_path(value: str, *, field: str) -> Path:
    path = Path(str(value or "").strip()).expanduser()
    if not str(value or "").strip() or not path.is_absolute():
        raise AirflowException(f"{field} must be an absolute path")
    return path.resolve()


def _path_under(path: Path, root: Path, *, field: str) -> Path:
    resolved_root = root.resolve()
    if not path.is_relative_to(resolved_root):
        raise AirflowException(f"{field} must be under {resolved_root}")
    return path


def _cycle_id(run_id: str) -> str:
    raw = str(run_id or "").strip()
    if not raw:
        raise AirflowException("Airflow run_id is required")
    return (
        "tm-registry-"
        + hashlib.sha256(
            f"{DAG_ID}:{raw}".encode("utf-8"),
        ).hexdigest()[:24]
    )


def _run_id(context: Mapping[str, Any]) -> str:
    dag_run = context.get("dag_run")
    value = getattr(dag_run, "run_id", None)
    if not value:
        raise AirflowException("dag_run.run_id is required")
    return str(value)


def _load_packet(path: Path):
    from utils.transfermarkt_approval import (
        ApprovalPacket,
        ApprovalValidationError,
    )

    try:
        value = json.loads(path.read_text(encoding="utf-8"))
        if not isinstance(value, Mapping):
            raise TypeError("packet root is not an object")
        return ApprovalPacket(**dict(value))
    except (
        OSError,
        json.JSONDecodeError,
        TypeError,
        ApprovalValidationError,
    ) as exc:
        raise AirflowException(f"approval packet is invalid: {path}: {exc}") from exc


def _assert_approved(journal, packet, *, packet_hash: str) -> None:
    from utils.transfermarkt_approval import ApprovalStateError

    try:
        record = journal.get(packet_hash)
    except ApprovalStateError as exc:
        raise AirflowException(f"approval packet is unavailable: {exc}") from exc
    if record.status != "approved":
        raise AirflowException(
            f"approval packet {packet.packet_id!r} is not approved: {record.status}"
        )
    if (
        record.packet_hash != packet.packet_hash
        or record.packet_id != packet.packet_id
        or record.canonical_json != packet.canonical_json
    ):
        raise AirflowException("approval journal content drift")


def _proxy_control_url() -> str:
    value = os.environ.get("TM_PROXY_CONTROL_URL", "").strip().rstrip("/")
    parsed = urlsplit(value)
    if parsed.scheme not in {"http", "https"} or not parsed.hostname:
        raise AirflowException("TM_PROXY_CONTROL_URL must be an HTTP(S) lease API")
    if parsed.hostname in {"transfermarkt.com", "www.transfermarkt.com"}:
        raise AirflowException("TM_PROXY_CONTROL_URL cannot be a source URL")
    return value


def _discovery_argv(
    *,
    cycle_id: str,
    run_id: str,
    proxy_control_url: str,
    checkpoint: Path,
    cache: Path,
    output_root: Path,
    paid_packet: Path,
    bronze_packet: Path,
    journal: Path,
) -> tuple[str, ...]:
    return (
        DISCOVERY_SCRIPT,
        "--cycle-id",
        cycle_id,
        "--dag-id",
        DAG_ID,
        "--run-id",
        run_id,
        "--task-id",
        DISCOVERY_TASK_ID,
        "--proxy-control-url",
        proxy_control_url,
        "--checkpoint",
        str(checkpoint),
        "--cache",
        str(cache),
        "--output-root",
        str(output_root),
        "--request-limit",
        str(PROXY_REQUEST_LIMIT),
        "--retry-limit",
        str(PROXY_RETRY_LIMIT),
        "--cache-ttl-seconds",
        str(CACHE_TTL_SECONDS),
        "--lease-ttl-seconds",
        str(LEASE_TTL_SECONDS),
        "--paid-proxy-approval-packet",
        str(paid_packet),
        "--production-write-approval-packet",
        str(bronze_packet),
        "--approval-journal",
        str(journal),
    )


def _validate_discovery_packet(
    packet,
    *,
    expected_action: str,
    expected_argv: Sequence[str],
    affected_files: Sequence[str],
) -> None:
    expected_tables = BRONZE_TABLES if expected_action == "production_write" else ()
    if packet.action != expected_action:
        raise AirflowException(f"approval action must be {expected_action}")
    if tuple(packet.argv) != tuple(expected_argv):
        raise AirflowException("discovery approval argv drift")
    if packet.byte_cap_bytes != PROVIDER_HARD_CAP_BYTES:
        raise AirflowException("discovery approval byte cap drift")
    if packet.request_limit != PROXY_REQUEST_LIMIT:
        raise AirflowException("discovery approval request limit drift")
    if packet.retry_limit != PROXY_RETRY_LIMIT:
        raise AirflowException("discovery approval retry limit drift")
    if packet.concurrency != PROXY_CONCURRENCY:
        raise AirflowException("discovery approval concurrency must be 1")
    if tuple(sorted(packet.affected_tables)) != tuple(sorted(expected_tables)):
        raise AirflowException("discovery approval table assets drift")
    if tuple(sorted(packet.affected_files)) != tuple(sorted(affected_files)):
        raise AirflowException("discovery approval file assets drift")


def _prepare_discovery(
    *,
    paid_proxy_packet_path: str,
    paid_proxy_packet_hash: str,
    bronze_write_packet_path: str,
    bronze_write_packet_hash: str,
    promotion_write_packet_path: str,
    approval_journal: str,
    **context,
) -> dict[str, str]:
    """Validate both discovery approvals without consuming either packet."""

    from utils.transfermarkt_approval import ApprovalJournal

    run_id = _run_id(context)
    cycle_id = _cycle_id(run_id)
    paid_path = _path_under(
        _absolute_path(paid_proxy_packet_path, field="paid_proxy_packet_path"),
        APPROVAL_ROOT,
        field="paid_proxy_packet_path",
    )
    bronze_path = _path_under(
        _absolute_path(bronze_write_packet_path, field="bronze_write_packet_path"),
        APPROVAL_ROOT,
        field="bronze_write_packet_path",
    )
    promotion_path = _path_under(
        _absolute_path(
            promotion_write_packet_path,
            field="promotion_write_packet_path",
        ),
        APPROVAL_ROOT,
        field="promotion_write_packet_path",
    )
    journal_path = _path_under(
        _absolute_path(approval_journal, field="approval_journal"),
        APPROVAL_ROOT,
        field="approval_journal",
    )
    if len({paid_path, bronze_path, promotion_path, journal_path}) != 4:
        raise AirflowException("approval packet and journal paths must be distinct")

    paid_hash = _digest(paid_proxy_packet_hash, field="paid_proxy_packet_hash")
    bronze_hash = _digest(
        bronze_write_packet_hash,
        field="bronze_write_packet_hash",
    )
    if paid_hash == bronze_hash:
        raise AirflowException("paid and Bronze write approvals must be distinct")

    checkpoint = STATE_ROOT / "checkpoints" / f"{cycle_id}.json"
    proxy_url = _proxy_control_url()
    argv = _discovery_argv(
        cycle_id=cycle_id,
        run_id=run_id,
        proxy_control_url=proxy_url,
        checkpoint=checkpoint,
        cache=CACHE_PATH,
        output_root=OUTPUT_ROOT,
        paid_packet=paid_path,
        bronze_packet=bronze_path,
        journal=journal_path,
    )
    affected_files = (
        str(checkpoint),
        str(CACHE_PATH),
        str(OUTPUT_ROOT),
        str(journal_path),
    )
    journal = ApprovalJournal(journal_path)
    packets = (
        (
            _load_packet(paid_path),
            paid_hash,
            "paid_proxy",
        ),
        (
            _load_packet(bronze_path),
            bronze_hash,
            "production_write",
        ),
    )
    if len({packet.packet_id for packet, _, _ in packets}) != 2:
        raise AirflowException("discovery approval packet ids must be distinct")
    for packet, packet_hash, action in packets:
        if packet.packet_hash != packet_hash:
            raise AirflowException("presented discovery approval hash drift")
        _validate_discovery_packet(
            packet,
            expected_action=action,
            expected_argv=argv,
            affected_files=affected_files,
        )
        _assert_approved(journal, packet, packet_hash=packet_hash)

    return {
        "TM_CYCLE_ID": cycle_id,
        "TM_DAG_ID": DAG_ID,
        "TM_RUN_ID": run_id,
        "TM_TASK_ID": DISCOVERY_TASK_ID,
        "TM_PROXY_CONTROL_URL": proxy_url,
        "TM_CHECKPOINT": str(checkpoint),
        "TM_CACHE": str(CACHE_PATH),
        "TM_OUTPUT_ROOT": str(OUTPUT_ROOT),
        "TM_REQUEST_LIMIT": str(PROXY_REQUEST_LIMIT),
        "TM_RETRY_LIMIT": str(PROXY_RETRY_LIMIT),
        "TM_CACHE_TTL_SECONDS": str(CACHE_TTL_SECONDS),
        "TM_LEASE_TTL_SECONDS": str(LEASE_TTL_SECONDS),
        "TM_PAID_PACKET": str(paid_path),
        "TM_BRONZE_PACKET": str(bronze_path),
        "TM_APPROVAL_JOURNAL": str(journal_path),
        "TM_PAID_APPROVAL_PRESENTED_HASH": paid_hash,
        "TM_WRITE_APPROVAL_PRESENTED_HASH": bronze_hash,
        "TM_REQUIRE_METERED_PROXY": "true",
    }


def _parse_discovery_result(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        result = dict(value)
    elif isinstance(value, str):
        try:
            result = json.loads(value.strip().splitlines()[-1])
        except (IndexError, json.JSONDecodeError) as exc:
            raise AirflowException("discovery task returned invalid JSON") from exc
    else:
        raise AirflowException("discovery task returned no manifest reference")
    if result.get("status") != "success":
        raise AirflowException("discovery task did not report success")
    _digest(result.get("manifest_hash", ""), field="discovery manifest_hash")
    return result


def _load_discovery_manifest(
    task_result: Any,
    *,
    cycle_id: str,
) -> tuple[Path, str, dict[str, Any]]:
    from utils.transfermarkt_registry_publish import stable_hash

    result = _parse_discovery_result(task_result)
    manifest_path = _absolute_path(
        result.get("manifest_path", ""),
        field="discovery manifest_path",
    )
    expected_dir = (OUTPUT_ROOT / cycle_id).resolve()
    _path_under(manifest_path, expected_dir, field="discovery manifest_path")
    try:
        wrapper = json.loads(manifest_path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise AirflowException("discovery manifest is unreadable") from exc
    if not isinstance(wrapper, Mapping) or not isinstance(
        wrapper.get("manifest"),
        Mapping,
    ):
        raise AirflowException("discovery manifest wrapper is invalid")
    manifest = dict(wrapper["manifest"])
    manifest_hash = _digest(
        str(wrapper.get("manifest_hash") or ""),
        field="persisted discovery manifest_hash",
    )
    if (
        manifest_hash != result["manifest_hash"]
        or stable_hash(manifest) != manifest_hash
    ):
        raise AirflowException("discovery manifest content hash drift")
    if manifest.get("cycle_id") != cycle_id:
        raise AirflowException("discovery manifest cycle_id drift")
    return manifest_path, manifest_hash, manifest


def _promotion_argv(
    *,
    run_id: str,
    cycle_id: str,
    expected_revision: int,
    manifest_hash: str,
    registry_manifest_hash: str,
) -> tuple[str, ...]:
    return (
        "airflow",
        "tasks",
        "run",
        DAG_ID,
        "publish_registry",
        run_id,
        "--cycle-id",
        cycle_id,
        "--expected-revision",
        str(expected_revision),
        "--discovery-manifest-hash",
        manifest_hash,
        "--registry-manifest-hash",
        registry_manifest_hash,
    )


def _publication_manifest_path(
    *,
    cycle_id: str,
    registry_manifest_hash: str,
) -> Path:
    return (
        OUTPUT_ROOT
        / cycle_id
        / f"transfermarkt-registry-publish-{registry_manifest_hash}.json"
    ).resolve()


def _atomic_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temporary = tempfile.mkstemp(
        prefix=f".{path.name}.",
        suffix=".tmp",
        dir=path.parent,
    )
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            json.dump(
                value,
                handle,
                ensure_ascii=False,
                sort_keys=True,
                separators=(",", ":"),
                default=str,
            )
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temporary, path)
    except BaseException:
        try:
            os.close(descriptor)
        except OSError:
            pass
        try:
            os.unlink(temporary)
        except FileNotFoundError:
            pass
        raise


def _connect_trino():
    from utils.transfermarkt_native_v2 import connect

    return connect()


def _publish_registry(
    *,
    expected_revision: int,
    promotion_write_packet_path: str,
    promotion_write_packet_hash: str = "",
    approval_journal: str,
    connection_factory=None,
    publisher=None,
    **context,
) -> dict[str, Any]:
    """Apply Silver registry transforms and CAS after the third approval."""

    from utils.transfermarkt_approval import (
        ApprovalJournal,
        ApprovalStateError,
    )
    from utils import transfermarkt_registry_publish as registry_publish

    if isinstance(expected_revision, bool) or int(expected_revision) < 0:
        raise AirflowException("expected_revision must be a non-negative integer")
    revision = int(expected_revision)
    run_id = _run_id(context)
    cycle_id = _cycle_id(run_id)
    ti = context.get("ti")
    if ti is None:
        raise AirflowException("task instance context is required")
    manifest_path, manifest_hash, manifest = _load_discovery_manifest(
        ti.xcom_pull(task_ids=DISCOVERY_TASK_ID),
        cycle_id=cycle_id,
    )
    rows = manifest.get("rows")
    if not isinstance(rows, Mapping):
        raise AirflowException("discovery row evidence is missing")
    try:
        competition_count = int(rows["competitions"])
        edition_count = int(rows["competition_editions"])
    except (KeyError, TypeError, ValueError, OverflowError) as exc:
        raise AirflowException("discovery row evidence is invalid") from exc

    publish_fn = publisher or registry_publish.publish_registry
    # This call performs strict manifest/unknown checks and only renders SQL.
    # It cannot open a connection or execute a statement.
    planned = publish_fn(
        manifest,
        manifest_hash=manifest_hash,
        snapshot_id=str(manifest.get("snapshot_id") or ""),
        competition_count=competition_count,
        edition_count=edition_count,
        expected_revision=revision,
        apply=False,
    )
    publication_path = _publication_manifest_path(
        cycle_id=cycle_id,
        registry_manifest_hash=planned.plan.registry_manifest_hash,
    )

    packet_path = _path_under(
        _absolute_path(
            promotion_write_packet_path,
            field="promotion_write_packet_path",
        ),
        APPROVAL_ROOT,
        field="promotion_write_packet_path",
    )
    journal_path = _path_under(
        _absolute_path(approval_journal, field="approval_journal"),
        APPROVAL_ROOT,
        field="approval_journal",
    )
    packet = _load_packet(packet_path)
    presented_hash = (
        _digest(
            promotion_write_packet_hash,
            field="promotion_write_packet_hash",
        )
        if str(promotion_write_packet_hash or "").strip()
        else packet.packet_hash
    )
    argv = _promotion_argv(
        run_id=run_id,
        cycle_id=cycle_id,
        expected_revision=revision,
        manifest_hash=manifest_hash,
        registry_manifest_hash=planned.plan.registry_manifest_hash,
    )
    affected_tables = tuple(
        sorted(
            {
                registry_publish.COMPETITIONS_TABLE,
                registry_publish.EDITIONS_TABLE,
                registry_publish.REGISTRY_STATE_TABLE,
                *(table for _, table in planned.plan.staging_tables),
            }
        )
    )
    affected_files = tuple(
        sorted(
            (
                str(journal_path),
                str(manifest_path),
                str(publication_path),
            )
        )
    )
    if packet.packet_hash != presented_hash:
        raise AirflowException("presented promotion approval hash drift")
    if packet.action != "production_write":
        raise AirflowException("promotion approval action must be production_write")
    if tuple(packet.argv) != argv:
        raise AirflowException("promotion approval argv drift")
    if packet.byte_cap_bytes != 0 or packet.request_limit != 0:
        raise AirflowException("promotion approval must authorize zero proxy I/O")
    if packet.retry_limit != 0 or packet.concurrency != 1:
        raise AirflowException("promotion approval retry/concurrency drift")
    if tuple(sorted(packet.affected_tables)) != affected_tables:
        raise AirflowException("promotion approval table assets drift")
    if tuple(sorted(packet.affected_files)) != affected_files:
        raise AirflowException("promotion approval file assets drift")

    journal = ApprovalJournal(journal_path)
    _assert_approved(journal, packet, packet_hash=presented_hash)
    try:
        journal.consume(
            packet,
            presented_hash=presented_hash,
            execution_argv=argv,
        )
    except ApprovalStateError as exc:
        raise AirflowException(f"promotion approval is not consumable: {exc}") from exc

    connection = None
    try:
        # Re-render from disk after approval consumption. A deployment or SQL
        # edit between planning and execution invalidates the packet before a
        # Trino connection can be opened.
        revalidated = publish_fn(
            manifest,
            manifest_hash=manifest_hash,
            snapshot_id=str(manifest["snapshot_id"]),
            competition_count=competition_count,
            edition_count=edition_count,
            expected_revision=revision,
            apply=False,
        )
        if (
            revalidated.plan.registry_manifest_hash
            != planned.plan.registry_manifest_hash
        ):
            raise AirflowException("registry publication plan changed after approval")
        # The production connection is deliberately created only after the
        # exact approval has become consumed.
        connection = (connection_factory or _connect_trino)()
        applied = publish_fn(
            manifest,
            manifest_hash=manifest_hash,
            snapshot_id=str(manifest["snapshot_id"]),
            competition_count=competition_count,
            edition_count=edition_count,
            expected_revision=revision,
            apply=True,
            connection=connection,
        )
        evidence = {
            "status": "success",
            "cycle_id": cycle_id,
            "run_id": run_id,
            "discovery_manifest_path": str(manifest_path),
            "discovery_manifest_hash": manifest_hash,
            "promotion_approval_packet_hash": presented_hash,
            "publication": applied.as_dict(),
        }
        publication_hash = registry_publish.stable_hash(evidence)
        _atomic_json(
            publication_path,
            {"manifest_hash": publication_hash, "manifest": evidence},
        )
        return {
            "status": "success",
            "registry_snapshot_id": applied.plan.snapshot_id,
            "registry_revision": applied.plan.promoted_revision,
            "registry_manifest_hash": applied.plan.registry_manifest_hash,
            "publication_manifest_path": str(publication_path),
            "publication_manifest_hash": publication_hash,
            "dq": dict(applied.dq),
        }
    except BaseException as exc:
        try:
            journal.fail(
                packet,
                presented_hash=presented_hash,
                reason=str(exc).strip()[:1000] or "registry publication failed",
            )
        except ApprovalStateError:
            pass
        raise
    finally:
        if connection is not None:
            connection.close()


with DAG(
    dag_id=DAG_ID,
    default_args=SCRAPER_ARGS,
    description="Proxy-only Transfermarkt registry discovery and strict CAS promotion",
    # Registry taxonomy changes slowly. Run before the weekly Monday crawl,
    # but only once per month to avoid unnecessary residential traffic.
    schedule="0 2 1 * *",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    render_template_as_native_obj=True,
    tags=DAG_TAGS.get(
        "transfermarkt",
        ["scraping", "transfermarkt", "bronze", "football"],
    )
    + ["registry", "discovery"],
    max_active_runs=1,
    params={
        "paid_proxy_packet_path": Param(default="", type="string"),
        "paid_proxy_packet_hash": Param(default="", type="string"),
        "bronze_write_packet_path": Param(default="", type="string"),
        "bronze_write_packet_hash": Param(default="", type="string"),
        "promotion_write_packet_path": Param(
            default=str(APPROVAL_ROOT / "registry-promotion.json"),
            type="string",
        ),
        "promotion_write_packet_hash": Param(default="", type="string"),
        "approval_journal": Param(default=str(APPROVAL_JOURNAL), type="string"),
        "expected_registry_revision": Param(
            default=0,
            type="integer",
            minimum=0,
        ),
    },
    doc_md="""
    Monthly central-registry refresh. The scheduled run fails closed until
    separate approved paid-proxy and Bronze-write packets are supplied. The
    proxy task is single-concurrency in `transfermarkt_proxy` and has a hard
    15 MiB provider ledger. After discovery, create/approve the third packet
    for the exact manifest and clear only `publish_registry`; no Silver table
    or CAS state is touched before that packet is consumed. Unknown or
    conflicting active classification blocks publication.
    """,
) as dag:
    prepare_discovery_task = PythonOperator(
        task_id="prepare_discovery",
        python_callable=_prepare_discovery,
        op_kwargs={
            "paid_proxy_packet_path": "{{ params.paid_proxy_packet_path }}",
            "paid_proxy_packet_hash": "{{ params.paid_proxy_packet_hash }}",
            "bronze_write_packet_path": "{{ params.bronze_write_packet_path }}",
            "bronze_write_packet_hash": "{{ params.bronze_write_packet_hash }}",
            "promotion_write_packet_path": ("{{ params.promotion_write_packet_path }}"),
            "approval_journal": "{{ params.approval_journal }}",
        },
        retries=0,
    )

    discover_registry_task = BashOperator(
        task_id=DISCOVERY_TASK_ID,
        bash_command=r'''set -euo pipefail
cd /opt/airflow
exec python /opt/airflow/dags/scripts/run_transfermarkt_discovery.py \
  --cycle-id "$TM_CYCLE_ID" \
  --dag-id "$TM_DAG_ID" \
  --run-id "$TM_RUN_ID" \
  --task-id "$TM_TASK_ID" \
  --proxy-control-url "$TM_PROXY_CONTROL_URL" \
  --checkpoint "$TM_CHECKPOINT" \
  --cache "$TM_CACHE" \
  --output-root "$TM_OUTPUT_ROOT" \
  --request-limit "$TM_REQUEST_LIMIT" \
  --retry-limit "$TM_RETRY_LIMIT" \
  --cache-ttl-seconds "$TM_CACHE_TTL_SECONDS" \
  --lease-ttl-seconds "$TM_LEASE_TTL_SECONDS" \
  --paid-proxy-approval-packet "$TM_PAID_PACKET" \
  --production-write-approval-packet "$TM_BRONZE_PACKET" \
  --approval-journal "$TM_APPROVAL_JOURNAL"''',
        env="{{ ti.xcom_pull(task_ids='prepare_discovery') }}",
        append_env=True,
        retries=0,
        pool="transfermarkt_proxy",
        pool_slots=1,
        max_active_tis_per_dag=1,
        execution_timeout=timedelta(hours=2),
        do_xcom_push=True,
    )

    publish_registry_task = PythonOperator(
        task_id="publish_registry",
        python_callable=_publish_registry,
        op_kwargs={
            "expected_revision": "{{ params.expected_registry_revision }}",
            "promotion_write_packet_path": ("{{ params.promotion_write_packet_path }}"),
            "promotion_write_packet_hash": ("{{ params.promotion_write_packet_hash }}"),
            "approval_journal": "{{ params.approval_journal }}",
        },
        retries=0,
        execution_timeout=timedelta(minutes=30),
    )

    prepare_discovery_task >> discover_registry_task >> publish_registry_task


__all__ = [
    "DAG_ID",
    "dag",
    "_cycle_id",
    "_discovery_argv",
    "_prepare_discovery",
    "_promotion_argv",
    "_publish_registry",
]
