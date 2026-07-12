"""Shared Bronze-flatten helpers for the SofaScore source (#840).

Lightweight (stdlib-only) so both the heavy ``scraper`` module and the
lightweight ``camoufox_capture`` module can import it without a circular
dependency. The medallion contract: Bronze keeps SofaScore fields as-is
(``camelCase`` keys -> ``snake_case`` columns, nested objects flattened with a
path prefix); renames / derivations live in Silver.
"""

from __future__ import annotations

import re


# camelCase -> snake_case for Bronze column names.
_CAMEL_RE_1 = re.compile(r'([A-Z]+)([A-Z][a-z])')
_CAMEL_RE_2 = re.compile(r'([a-z\d])([A-Z])')


def _camel_to_snake(name: str) -> str:
    """Convert a camelCase / PascalCase key to snake_case.

    Examples:
        ``goalsPrevented`` -> ``goals_prevented``
        ``XGOnTarget``     -> ``xg_on_target``
        ``totalAttemptAssist`` -> ``total_attempt_assist``
    """
    s1 = _CAMEL_RE_1.sub(r'\1_\2', name)
    return _CAMEL_RE_2.sub(r'\1_\2', s1).lower()


def _coerce_scalar(v):
    """Coerce a JSON value to a Bronze-safe scalar.

    SofaScore stats often nest a structure like
    ``{"value": 3, "previousValue": 2, ...}`` for richer UI rendering.
    For Bronze we only keep the canonical ``value``; richer payloads
    can be re-derived from raw JSON if ever needed.
    """
    if isinstance(v, dict):
        # Most common SofaScore shape: {"key": "...", "value": ...}.
        if 'value' in v:
            return _coerce_scalar(v['value'])
        return None
    if isinstance(v, (list, tuple)):
        return None
    if isinstance(v, bool):
        return v
    if isinstance(v, (int, float)):
        return v
    if v is None:
        return None
    # String — try numeric upcast (SofaScore returns e.g. "12.4" for some
    # rating sub-stats). Fall back to the raw string.
    s = str(v).strip()
    if not s:
        return None
    try:
        if '.' in s:
            return float(s)
        return int(s)
    except (TypeError, ValueError):
        return s


# Bronze payloads nest shallowly (SofaScore: ~2-3 levels). Cap recursion so a
# pathological / cyclic payload can never blow the stack or explode column count.
_MAX_FLATTEN_DEPTH = 4


def _auto_flatten(payload, out, prefix='', skip=(), _depth=0):
    """Recursively flatten a SofaScore dict into Bronze-safe snake_case scalar
    columns, IN PLACE on ``out`` (#840). Generalises the inline loop in
    the common SofaScore payload parsers.

    Rules:
        * scalar (str/int/float/bool/None) -> out[prefix+snake] = _coerce_scalar(v)
        * dict WITH a ``value`` wrapper     -> out[prefix+snake] = _coerce_scalar(v)
          (SofaScore's ``{"value": X, "previousValue": Y, ...}`` UI shape -> X)
        * plain nested dict (NO ``value``)  -> recurse, prefix = snake + '_'
          (e.g. ``playerCoordinates.x`` -> ``player_coordinates_x``)
        * list / tuple                      -> skipped (Bronze stays flat)

    ``skip`` lists TOP-LEVEL keys to ignore entirely (identity objects already
    projected as hard-coded anchors, e.g. ``player`` / ``team``). Keys already
    present in ``out`` (the PK / identity anchors) are NEVER overwritten, so the
    anchor types (stringified ids, coerced bools) stay authoritative.
    """
    if not isinstance(payload, dict) or _depth > _MAX_FLATTEN_DEPTH:
        return out
    for raw_key, raw_val in payload.items():
        if raw_key in skip:
            continue
        if isinstance(raw_val, (list, tuple)):
            # Bronze stays flat — arrays aren't projected to columns (would only
            # yield an all-NULL column via _coerce_scalar; re-derive from raw JSON).
            continue
        col = f"{prefix}{_camel_to_snake(str(raw_key))}"
        if isinstance(raw_val, dict) and 'value' not in raw_val:
            # Plain nested object -> recurse with a path prefix. ``skip`` is
            # deliberately NOT propagated (it targets top-level identity keys).
            _auto_flatten(raw_val, out, prefix=f"{col}_", _depth=_depth + 1)
        else:
            # Scalar, or a {"value": ...} wrapper -> Bronze scalar.
            if col in out:
                continue  # never clobber a hard-coded anchor
            out[col] = _coerce_scalar(raw_val)
    return out
