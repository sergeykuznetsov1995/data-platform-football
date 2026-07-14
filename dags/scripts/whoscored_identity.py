"""Collision-resistant filesystem/S3 identity helpers for WhoScored DAGs."""

from __future__ import annotations

import hashlib
import re


def stable_safe_token(value: object, *, max_length: int = 120) -> str:
    """Return a readable token that always binds the complete source value."""

    if max_length < 18:
        raise ValueError("safe token max_length must be at least 18")
    source = str(value)
    rendered = re.sub(r"[^A-Za-z0-9_.-]+", "_", source).strip("._") or "unknown"
    digest = hashlib.sha256(source.encode("utf-8")).hexdigest()[:16]
    prefix = rendered[: max_length - len(digest) - 1].rstrip("._-") or "unknown"
    return f"{prefix}-{digest}"
