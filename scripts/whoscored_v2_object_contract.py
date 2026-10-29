"""Authoritative object inventory for the WhoScored V2 cutover tools.

This module deliberately contains no Trino calls.  Migration and cleanup use
the same immutable inventory, while a unit test compares it with the runtime
repository constants.  A new WhoScored dataset therefore cannot be added to
the parser and silently omitted from rollback or cleanup validation.
"""

from __future__ import annotations

# ruff: noqa: E402 -- the trust anchor must run before every non-built-in import

import sys as _whoscored_bootstrap_sys

_whoscored_source = __file__
if not _whoscored_source.startswith("/"):
    raise RuntimeError("WhoScored entrypoint requires an absolute source path")
_whoscored_production = _whoscored_source.startswith("/opt/airflow/")
_whoscored_root = (
    "/opt/airflow"
    if _whoscored_production
    else _whoscored_source.rsplit("/scripts/", 1)[0]
)
if _whoscored_production:
    if getattr(
        _whoscored_bootstrap_sys, "_whoscored_runtime_startup_schema", None
    ) != 2:
        raise RuntimeError("image-baked WhoScored startup anchor is required")
elif (
    getattr(_whoscored_bootstrap_sys, "_whoscored_runtime_startup_root", None)
    != _whoscored_root
):
    _whoscored_anchor_path = (
        _whoscored_root + "/docker/images/airflow/whoscored_runtime_startup.py"
    )
    _whoscored_anchor_globals = {
        "__builtins__": __builtins__,
        "sys": _whoscored_bootstrap_sys,
        "_WHOSCORED_RUNTIME_ROOT": _whoscored_root,
        "_WHOSCORED_REQUIRE_FULL_ATTESTATION": False,
    }
    with open(_whoscored_anchor_path, "rb") as _whoscored_anchor_handle:
        _whoscored_anchor_source = _whoscored_anchor_handle.read()
    exec(
        compile(_whoscored_anchor_source, _whoscored_anchor_path, "exec"),
        _whoscored_anchor_globals,
    )
_WHOSCORED_RUNTIME_CONTRACT = (
    _whoscored_bootstrap_sys._load_whoscored_runtime_contract(_whoscored_root)
)
_WHOSCORED_RUNTIME_CONTRACT.require_production_runtime_class(
    operation="WhoScored V2 object contract"
)

from scripts.whoscored_v2_inventory import *  # noqa: F403 - protected compatibility entry point
