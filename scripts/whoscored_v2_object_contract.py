"""Authoritative object inventory for the WhoScored V2 cutover tools.

This module deliberately contains no Trino calls.  Migration and cleanup use
the same immutable inventory, while a unit test compares it with the runtime
repository constants.  A new WhoScored dataset therefore cannot be added to
the parser and silently omitted from rollback or cleanup validation.
"""

from __future__ import annotations

from scripts.whoscored_v2_inventory import *  # noqa: F403 - protected compatibility entry point
