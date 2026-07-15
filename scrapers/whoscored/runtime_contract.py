"""Fail-closed compatibility contract for a deployed WhoScored worker tree.

The Airflow services use bind-mounted source code, so files from two releases
can otherwise be combined in one Python process.  The contract pins the small
set of modules that jointly implement parsing and persistence and also checks
the interfaces that caused the last production failure.
"""

from __future__ import annotations

import hashlib
import inspect
import json
import re
from pathlib import Path
from typing import Any, Mapping, Optional


RUNTIME_CONTRACT_PATH = Path(__file__).with_name("runtime_contract.lock")
_SHA256_RE = re.compile(r"[0-9a-f]{64}")


class RuntimeContractError(RuntimeError):
    """Raised when mounted WhoScored code is not one coherent release."""


def _load_contract(path: Path) -> Mapping[str, Any]:
    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise RuntimeContractError(
            f"cannot load WhoScored runtime contract {path}: {exc}"
        ) from exc
    if not isinstance(payload, dict):
        raise RuntimeContractError("WhoScored runtime contract must be a JSON object")
    return payload


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    try:
        with path.open("rb") as handle:
            for chunk in iter(lambda: handle.read(1024 * 1024), b""):
                digest.update(chunk)
    except OSError as exc:
        raise RuntimeContractError(f"cannot hash required runtime file {path}: {exc}") from exc
    return digest.hexdigest()


def validate_runtime_contract(
    *,
    contract_path: Optional[Path] = None,
    runtime_root: Optional[Path] = None,
    report_schema_version: Optional[int] = None,
) -> dict[str, Any]:
    """Validate file identity, parser/report schemas, and writer compatibility."""

    from scrapers.base.iceberg_writer import IcebergWriter
    from scrapers.whoscored.parsers import PARSER_VERSION
    from scrapers.whoscored.repository import WHOSCORED_BUSINESS_TABLES

    if report_schema_version is None:
        from dags.scripts.run_whoscored_scraper import REPORT_SCHEMA_VERSION

        actual_report_schema_version = REPORT_SCHEMA_VERSION
    else:
        actual_report_schema_version = int(report_schema_version)

    path = (contract_path or RUNTIME_CONTRACT_PATH).resolve()
    root = (runtime_root or Path(__file__).resolve().parents[2]).resolve()
    contract = _load_contract(path)
    expected_keys = {
        "schema_version",
        "parser_version",
        "report_schema_version",
        "business_dataset_count",
        "files",
    }
    if set(contract) != expected_keys or contract.get("schema_version") != 1:
        raise RuntimeContractError("invalid WhoScored runtime contract schema")
    if contract.get("parser_version") != PARSER_VERSION:
        raise RuntimeContractError(
            "WhoScored parser version mismatch: "
            f"expected={contract.get('parser_version')!r}, actual={PARSER_VERSION!r}"
        )
    if contract.get("report_schema_version") != actual_report_schema_version:
        raise RuntimeContractError(
            "WhoScored report schema version mismatch: "
            f"expected={contract.get('report_schema_version')!r}, "
            f"actual={actual_report_schema_version!r}"
        )
    if contract.get("business_dataset_count") != len(WHOSCORED_BUSINESS_TABLES):
        raise RuntimeContractError(
            "WhoScored business schema mismatch: "
            f"expected={contract.get('business_dataset_count')!r}, "
            f"actual={len(WHOSCORED_BUSINESS_TABLES)!r}"
        )

    signature = inspect.signature(IcebergWriter.write_dataframe)
    bulk_arrow = signature.parameters.get("bulk_arrow")
    if bulk_arrow is None or bulk_arrow.default is not False:
        raise RuntimeContractError(
            "IcebergWriter.write_dataframe must expose bulk_arrow=False"
        )

    files = contract.get("files")
    if not isinstance(files, dict) or not files:
        raise RuntimeContractError("WhoScored runtime contract files must be non-empty")
    actual_hashes: dict[str, str] = {}
    for relative, expected_hash in sorted(files.items()):
        if (
            not isinstance(relative, str)
            or not relative
            or Path(relative).is_absolute()
            or ".." in Path(relative).parts
            or not isinstance(expected_hash, str)
            or _SHA256_RE.fullmatch(expected_hash) is None
        ):
            raise RuntimeContractError("invalid WhoScored runtime file contract entry")
        candidate = (root / relative).resolve()
        try:
            candidate.relative_to(root)
        except ValueError as exc:
            raise RuntimeContractError(
                f"runtime contract file escapes root: {relative!r}"
            ) from exc
        actual_hash = _sha256(candidate)
        if actual_hash != expected_hash:
            raise RuntimeContractError(
                "WhoScored runtime file hash mismatch: "
                f"file={relative}, expected={expected_hash}, actual={actual_hash}"
            )
        actual_hashes[relative] = actual_hash

    identity = hashlib.sha256(
        json.dumps(
            actual_hashes,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")
    ).hexdigest()
    return {
        "status": "success",
        "parser_version": PARSER_VERSION,
        "report_schema_version": actual_report_schema_version,
        "business_dataset_count": len(WHOSCORED_BUSINESS_TABLES),
        "file_count": len(actual_hashes),
        "code_tree_sha256": identity,
    }


def _airflow_pool_slots(pool_name: str) -> int:
    """Read one persisted Airflow pool size without relying on CLI output."""

    try:
        from airflow.models.pool import Pool
        from airflow.utils.session import create_session

        with create_session() as session:
            pool = session.query(Pool).filter(Pool.pool == pool_name).one_or_none()
    except Exception as exc:
        raise RuntimeContractError(
            f"cannot read Airflow source pool {pool_name!r}: {exc}"
        ) from exc
    if pool is None:
        raise RuntimeContractError(f"Airflow source pool {pool_name!r} does not exist")
    return int(pool.slots)


def validate_airflow_source_pool(
    *,
    direct_pool: str,
    backfill_pool: str,
) -> dict[str, Any]:
    """Prove daily and backfill share the modeled physical source pool."""

    from scrapers.whoscored.runtime_limits import source_pool_slots

    if not direct_pool or direct_pool != backfill_pool:
        raise RuntimeContractError(
            "WhoScored daily and backfill must share one Airflow source pool"
        )
    try:
        expected_slots = source_pool_slots()
    except ValueError as exc:
        raise RuntimeContractError(str(exc)) from exc
    actual_slots = _airflow_pool_slots(direct_pool)
    if actual_slots != expected_slots:
        raise RuntimeContractError(
            "WhoScored Airflow source pool size mismatch: "
            f"pool={direct_pool!r}, expected={expected_slots}, actual={actual_slots}"
        )
    return {
        "pool": direct_pool,
        "expected_slots": expected_slots,
        "actual_slots": actual_slots,
    }
