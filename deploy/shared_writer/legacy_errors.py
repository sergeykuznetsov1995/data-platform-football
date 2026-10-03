"""Explicit, bounded exception for the investigated legacy WhoScored parse errors.

No live error is learned as an exemption. Changing these pins needs code review.
All active DAGs must remain error-free; the policy applies only to these six
inactive files in the old common runtime, not the separate WhoScored service.
"""
from __future__ import annotations

import hashlib
from pathlib import Path
import stat

POLICY = "whoscored-legacy-20261003"
POLICIES = ("none", POLICY)
ERRORS = {'/opt/airflow/dags/dag_backup_whoscored_storage.py': '405a4b65a4e226c0b6cc4ef9b5280206300d7f86aea399f34ffa86211572cb87',
 '/opt/airflow/dags/dag_canary_whoscored_proxy.py': '405a4b65a4e226c0b6cc4ef9b5280206300d7f86aea399f34ffa86211572cb87',
 '/opt/airflow/dags/scripts/run_whoscored_backfill_item.py': '405a4b65a4e226c0b6cc4ef9b5280206300d7f86aea399f34ffa86211572cb87',
 '/opt/airflow/dags/scripts/run_whoscored_scraper.py': '405a4b65a4e226c0b6cc4ef9b5280206300d7f86aea399f34ffa86211572cb87',
 '/opt/airflow/dags/scripts/whoscored_production_issuance.py': '405a4b65a4e226c0b6cc4ef9b5280206300d7f86aea399f34ffa86211572cb87',
 '/opt/airflow/dags/scripts/whoscored_proxy_runtime.py': '405a4b65a4e226c0b6cc4ef9b5280206300d7f86aea399f34ffa86211572cb87'}
PROTECTED = {'dags/.airflowignore': '5980c7cd16e2a81d194ecde45044806eed8f181bd52e863cfa1c13c3142ff2a7',
 'dags/dag_backup_whoscored_storage.py': '42c148a37097240896bc71b242e864081e3124b41355b4cddd526c78bfd2f258',
 'dags/dag_canary_whoscored_proxy.py': '95bf0432328a7575489ec7c3164e8b4e4226d2666643c8707c3f45a4f92cdff5',
 'dags/scripts/run_whoscored_backfill_item.py': 'ce01345aa02e4587c1c2239cbfeff62b9b1efaa777232941c16dc65b5cbbc436',
 'dags/scripts/run_whoscored_scraper.py': '3e8500161108061b54b1c5701fb80871083c63e9daf81fd71d6d9d9d5cbbf2f7',
 'dags/scripts/whoscored_production_issuance.py': 'f4ff4261e4b4c47100e7acc1629a199a008409c94053f65d67224ae84b4fbdf1',
 'dags/scripts/whoscored_proxy_runtime.py': '0895e9a1efb5412cde17b8518bfaa6c523bca57becfaa9e3059529ae2fba3855',
 'scrapers/whoscored/runtime_contract.lock': 'd8ffb3af1994de1e048d1e9ff8a8cc0d86b86d3091b8733f19a3f2da590ac72e',
 'scrapers/whoscored/runtime_contract.py': '42407580ea2b84b8e3ff41840d5664cb14364d5a9e7b4eae9f9bd19da6946ab2'}


def protected_hashes(root: Path, policy: str) -> dict[str, str]:
    if policy not in POLICIES:
        raise RuntimeError("unknown import-error policy")
    if policy == "none":
        return {}
    result = {}
    for relative, expected in PROTECTED.items():
        path = root / relative
        try:
            if path.resolve() != path or not stat.S_ISREG(path.lstat().st_mode):
                raise RuntimeError("legacy import-error protected path is not regular")
            actual = hashlib.sha256(path.read_bytes()).hexdigest()
        except OSError as exc:
            raise RuntimeError("cannot fingerprint legacy import-error protected file") from exc
        if actual != expected:
            raise RuntimeError("legacy import-error protected code drift")
        result[relative] = actual
    return result


def validate(snapshot: dict) -> None:
    policy = snapshot.get("import_error_policy", "none")
    if policy not in POLICIES:
        raise RuntimeError("unknown import-error policy")
    details = snapshot["import_error_details"]
    if policy == "none":
        if details:
            raise RuntimeError("unapproved import errors")
        return
    if snapshot.get("import_error_protected") != PROTECTED:
        raise RuntimeError("legacy import-error protected code drift")
    if len(details) != len(ERRORS) or {e["filename"]: e["sha256"] for e in details} != ERRORS:
        raise RuntimeError("legacy import-error baseline changed")
    if any(dag["fileloc"] in ERRORS for dag in snapshot["dags"].values()):
        raise RuntimeError("legacy import-error file belongs to an active DAG")
