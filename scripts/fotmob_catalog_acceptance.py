#!/usr/bin/env python3
"""Fail-closed acceptance for one automatic FotMob catalog report.

This validator is intentionally separate from :mod:`scripts.fotmob_acceptance`.
The latter is the frozen issue-930 cohort ceremony; this module validates only
the dynamically discovered ``fotmob-catalog-v1`` runner evidence.
"""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

from scrapers.fotmob.catalog_contract import (
    CATALOG_CONTRACT_SCHEMA,
    CatalogContract,
    catalog_contract_from_dict,
)
from scrapers.fotmob.scope_codec import format_scope_token, parse_scope_token


CATALOG_ACCEPTANCE_SCHEMA = "fotmob-catalog-acceptance-v1"
CLASSIFIER_VERSION = "fotmob-men-v1"
CURRENT_COMPLETION_MAX_AGE = timedelta(hours=72)

_MALE_GENDERS = frozenset({"m", "male", "man", "men"})
_ADULT_AGE_GROUPS = frozenset({"adult", "adults", "male", "men", "senior", "seniors"})
_KNOWN_TYPES = frozenset(
    {"competition", "cup", "friendly", "friendlies", "league", "tournament"}
)
_TERMINAL_SCOPE_OUTCOMES = frozenset({"success", "source_gap"})
_KNOWN_SCOPE_OUTCOMES = _TERMINAL_SCOPE_OUTCOMES | frozenset(
    {"retryable", "deferred", "terminal"}
)
_FEMALE_RE = re.compile(
    r"(?:\bwomen(?:'s)?\b|\bwoman\b|\bfemale\b|\bfeminine\b|"
    r"\bfemenin[oa]\b|\bfrauen\b|\bdamer\b|\bdonne\b|\bladies\b)",
    re.IGNORECASE,
)
_YOUTH_RE = re.compile(
    r"(?:\bu\s*-?\s*\d+\b|\bunder\s*-?\s*\d+\b|\byouth\b|"
    r"\bacademy\b|\bjunior(?:s)?\b|\bjuvenil\b|\bprimavera\b)",
    re.IGNORECASE,
)
_RESERVE_RE = re.compile(
    r"(?:\breserve(?:s)?\b|\bdevelopment\b|\bsecond teams?\b|"
    r"\bii teams?\b|\bpremier league 2(?:\s+div(?:ision)?\s+\d+)?\b)",
    re.IGNORECASE,
)
_SHOW_RE = re.compile(
    r"(?:\bcharit(?:y|ies)\b|\btestimonials?\b|\bexhibitions?\b|"
    r"\bshows?\b|\blegends?\b)",
    re.IGNORECASE,
)


@dataclass(frozen=True, slots=True)
class CatalogAcceptanceResult:
    """Pure, JSON-serializable result of validating one runner report."""

    ok: bool
    errors: tuple[str, ...]
    catalog_count: int
    included_count: int
    scope_count: int

    def __getitem__(self, key: str) -> Any:
        """Allow small callers to consume the result like its JSON object."""

        return self.as_dict()[key]

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": CATALOG_ACCEPTANCE_SCHEMA,
            "ok": self.ok,
            "errors": list(self.errors),
            "catalog_count": self.catalog_count,
            "included_count": self.included_count,
            "scope_count": self.scope_count,
        }


def _mapping(value: Any) -> Mapping[str, Any] | None:
    return value if isinstance(value, Mapping) else None


def _nonempty_string(value: Any) -> str | None:
    if not isinstance(value, str) or not value.strip():
        return None
    return value.strip()


def _parse_timestamp(value: Any, *, field: str, errors: list[str]) -> datetime | None:
    text = _nonempty_string(value)
    if text is None:
        errors.append(f"{field} must be a non-empty RFC3339 timestamp")
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
    except ValueError:
        errors.append(f"{field} must be a valid RFC3339 timestamp")
        return None
    if parsed.tzinfo is None or parsed.utcoffset() is None:
        errors.append(f"{field} must include a UTC offset")
        return None
    return parsed.astimezone(timezone.utc)


def _catalog_ids(value: Any, errors: list[str]) -> tuple[int, ...]:
    if not isinstance(value, list):
        errors.append("selection.catalog_ids must be an array")
        return ()
    valid = all(type(item) is int and item > 0 for item in value)
    if not valid:
        errors.append("selection.catalog_ids must contain only positive integer IDs")
        return ()
    if len(value) != len(set(value)):
        errors.append("selection.catalog_ids contains duplicate IDs")
    return tuple(value)


def _contract(
    value: Any, errors: list[str]
) -> tuple[CatalogContract | None, Mapping[str, Any]]:
    raw = _mapping(value)
    if raw is None:
        errors.append("selection.catalog_contract must be an object")
        return None, {}
    try:
        return catalog_contract_from_dict(raw), raw
    except (AttributeError, KeyError, TypeError, ValueError) as exc:
        errors.append(f"catalog contract does not recompute exactly: {exc}")
        return None, raw


def _decision_ids(
    catalog_ids: tuple[int, ...],
    decisions_value: Any,
    errors: list[str],
) -> tuple[list[Mapping[str, Any]], dict[int, Mapping[str, Any]]]:
    if not isinstance(decisions_value, list):
        errors.append("selection.catalog_decisions must be an array")
        return [], {}
    decisions: list[Mapping[str, Any]] = []
    ids: list[int] = []
    for index, raw in enumerate(decisions_value):
        decision = _mapping(raw)
        if decision is None:
            errors.append(f"catalog decision #{index} must be an object")
            continue
        competition_id = decision.get("competition_id")
        if type(competition_id) is not int or competition_id <= 0:
            errors.append(
                f"catalog decision #{index} must contain a positive competition_id"
            )
            continue
        decisions.append(decision)
        ids.append(competition_id)

    counts = Counter(ids)
    expected = set(catalog_ids)
    observed = set(ids)
    if any(counts[item] != 1 for item in expected) or observed != expected:
        errors.append(
            "every catalog ID must have exactly one decision "
            f"(catalog={sorted(expected)}, decisions={sorted(observed)}, "
            f"duplicates={sorted(item for item, count in counts.items() if count > 1)})"
        )
    return decisions, {
        competition_id: decision
        for competition_id, decision in zip(ids, decisions)
        if counts[competition_id] == 1
    }


def _has_structural_male_evidence(
    decision: Mapping[str, Any], *, classifier_version: str
) -> bool:
    gender = str(decision.get("source_gender") or "").strip().casefold()
    age_group = str(decision.get("source_age_group") or "").strip().casefold()
    source_type = str(decision.get("source_type") or "").strip().casefold()
    content_hash = str(decision.get("profile_content_hash") or "")
    return bool(
        decision.get("decision") == "included"
        and decision.get("probe_status") == "success"
        and decision.get("classifier_version") == classifier_version
        and decision.get("policy_rule") == "include_structural_male_adult"
        and gender in _MALE_GENDERS
        and (not age_group or age_group in _ADULT_AGE_GROUPS)
        and source_type in _KNOWN_TYPES
        and _nonempty_string(decision.get("profile_name"))
        and _nonempty_string(decision.get("profile_target_key"))
        and re.fullmatch(r"[0-9a-f]{64}", content_hash)
    )


def _forbidden_signal(decision: Mapping[str, Any]) -> str | None:
    text = " ".join(
        str(decision.get(field) or "")
        for field in (
            "catalog_name",
            "profile_name",
            "source_gender",
            "source_age_group",
            "source_type",
            "reason",
        )
    )
    for label, pattern in (
        ("female", _FEMALE_RE),
        ("youth", _YOUTH_RE),
        ("reserve", _RESERVE_RE),
        ("show", _SHOW_RE),
    ):
        if pattern.search(text):
            return label
    return None


def _planned_scopes(
    value: Any,
    *,
    included_ids: set[int],
    decision_by_id: Mapping[int, Mapping[str, Any]],
    errors: list[str],
) -> tuple[str, ...]:
    if not isinstance(value, list):
        errors.append("selection.planned_scopes must be an array")
        return ()
    parsed: list[str] = []
    for index, raw in enumerate(value):
        try:
            competition_id, season = parse_scope_token(raw)
            token = format_scope_token(competition_id, season)
        except (TypeError, ValueError) as exc:
            errors.append(f"planned scope #{index} is invalid: {exc}")
            continue
        parsed.append(token)
        if competition_id not in included_ids:
            errors.append(
                f"planned scope {token!r} is not owned by an included competition"
            )
            continue
        signal = _forbidden_signal(decision_by_id.get(competition_id, {}))
        if signal is not None:
            errors.append(
                f"planned scope {token!r} has forbidden structural signal: {signal}"
            )
    if len(parsed) != len(set(parsed)):
        errors.append("selection.planned_scopes contains duplicate exact scopes")
    return tuple(parsed)


def _scope_attempts(
    value: Any,
    *,
    contract_scopes: set[str],
    plan_signature: str | None,
    planned_scopes: set[str],
    lane: str | None,
    now: datetime,
    errors: list[str],
) -> dict[str, Mapping[str, Any]]:
    if not isinstance(value, list):
        errors.append("selection.scope_attempts must be an array")
        return {}
    by_scope: dict[str, Mapping[str, Any]] = {}
    soft_outcomes = False
    for index, raw in enumerate(value):
        attempt = _mapping(raw)
        if attempt is None:
            errors.append(f"scope attempt #{index} must be an object")
            continue
        try:
            token = format_scope_token(
                attempt.get("competition_id"), attempt.get("source_season_key")
            )
        except (TypeError, ValueError) as exc:
            errors.append(f"scope attempt #{index} has an invalid scope: {exc}")
            continue
        if token in by_scope:
            errors.append(f"scope {token!r} has duplicate latest-attempt evidence")
        else:
            by_scope[token] = attempt
        if token not in contract_scopes:
            errors.append(f"scope attempt {token!r} is outside the exact contract")
        if plan_signature is not None and attempt.get("plan_signature") != plan_signature:
            errors.append(f"scope attempt {token!r} has the wrong plan signature")

        attempt_count = attempt.get("attempt_count")
        if type(attempt_count) is not int or attempt_count < 1:
            errors.append(f"scope attempt {token!r} has an invalid attempt_count")
        last_attempt_at = _parse_timestamp(
            attempt.get("last_attempt_at"),
            field=f"scope attempt {token!r} last_attempt_at",
            errors=errors,
        )
        if last_attempt_at is not None and last_attempt_at > now:
            errors.append(f"scope attempt {token!r} is dated in the future")

        outcome = attempt.get("outcome")
        if outcome not in _KNOWN_SCOPE_OUTCOMES:
            errors.append(f"scope attempt {token!r} has unknown outcome {outcome!r}")
            continue
        reason = _nonempty_string(attempt.get("reason"))
        if outcome in {"retryable", "deferred"}:
            soft_outcomes = True
        if outcome == "retryable":
            next_retry_at = _parse_timestamp(
                attempt.get("next_retry_at"),
                field=f"retryable scope {token!r} next_retry_at",
                errors=errors,
            )
            if reason is None:
                errors.append(f"retryable scope {token!r} must record an explicit reason")
            if (
                next_retry_at is not None
                and last_attempt_at is not None
                and next_retry_at <= last_attempt_at
            ):
                errors.append(
                    f"retryable scope {token!r} next_retry_at must follow its attempt"
                )
        elif outcome == "source_gap":
            identities = attempt.get("attempt_identities")
            valid_identities = (
                isinstance(identities, list)
                and len(identities) == 2
                and all(_nonempty_string(item) is not None for item in identities)
            )
            if (
                type(attempt_count) is not int
                or attempt_count < 2
                or not valid_identities
                or (valid_identities and len(set(identities)) != 2)
            ):
                errors.append(
                    f"source_gap scope {token!r} must identify exactly two successful attempts"
                )
            if reason is None:
                errors.append(f"source_gap scope {token!r} must record an explicit reason")
        elif outcome == "terminal":
            errors.append(f"terminal scope {token!r} is a hard failure")
            if reason is None:
                errors.append(f"terminal scope {token!r} must record an explicit reason")
        elif outcome == "deferred" and reason is None:
            errors.append(f"{outcome} scope {token!r} must record an explicit reason")

    missing_planned = sorted(planned_scopes.difference(by_scope))
    if missing_planned:
        errors.append(
            "every planned scope must have durable attempt evidence: "
            + ", ".join(missing_planned)
        )

    if lane == "current":
        missing_current = sorted(contract_scopes.difference(by_scope))
        if missing_current:
            errors.append(
                "every current contract scope must have terminal completion evidence: "
                + ", ".join(missing_current)
            )
        for token in sorted(contract_scopes.intersection(by_scope)):
            attempt = by_scope[token]
            if attempt.get("outcome") not in _TERMINAL_SCOPE_OUTCOMES:
                errors.append(f"current scope {token!r} is not terminal")
                continue
            last_attempt_at = _parse_timestamp(
                attempt.get("last_attempt_at"),
                field=f"current scope {token!r} last_attempt_at",
                errors=[],
            )
            if last_attempt_at is not None and now - last_attempt_at > CURRENT_COMPLETION_MAX_AGE:
                errors.append(f"current scope {token!r} completion is older than 72 hours")

    # ``soft_outcomes`` is returned through a reserved sentinel so the caller
    # can verify the report-level status without widening this public helper.
    if soft_outcomes:
        by_scope["\0soft"] = {}
    return by_scope


def validate_report(
    report: Mapping[str, Any], *, now: datetime | None = None
) -> CatalogAcceptanceResult:
    """Validate one automatic runner report without performing any I/O."""

    errors: list[str] = []
    if not isinstance(report, Mapping):
        return CatalogAcceptanceResult(
            ok=False,
            errors=("runner report must be an object",),
            catalog_count=0,
            included_count=0,
            scope_count=0,
        )
    checked_at = now or datetime.now(timezone.utc)
    if checked_at.tzinfo is None or checked_at.utcoffset() is None:
        errors.append("acceptance now must include a UTC offset")
        checked_at = checked_at.replace(tzinfo=timezone.utc)
    checked_at = checked_at.astimezone(timezone.utc)

    status = report.get("status")
    if status not in {"success", "partial_success"}:
        errors.append("automatic report status must be success or partial_success")

    selection = _mapping(report.get("selection"))
    if selection is None:
        errors.append("runner report selection must be an object")
        selection = {}
    lane = selection.get("scope_lane")
    if lane not in {"current", "history"}:
        errors.append("selection.scope_lane must be current or history")
        lane = None

    contract, raw_contract = _contract(selection.get("catalog_contract"), errors)
    if raw_contract.get("schema") != CATALOG_CONTRACT_SCHEMA:
        errors.append(f"automatic acceptance requires {CATALOG_CONTRACT_SCHEMA}")
    included_values = contract.included_ids if contract is not None else ()
    if contract is None and isinstance(raw_contract.get("included_ids"), list):
        included_values = raw_contract["included_ids"]
    included_ids = {
        item
        for item in included_values or ()
        if type(item) is int and item > 0
    }
    raw_scopes = raw_contract.get("scopes")
    contract_scopes = set(contract.scopes if contract is not None else ())
    if contract is None and isinstance(raw_scopes, list):
        contract_scopes = {item for item in raw_scopes if isinstance(item, str)}
    plan_signature = (
        contract.plan_signature
        if contract is not None
        else _nonempty_string(raw_contract.get("plan_signature"))
    )
    classifier_version = (
        contract.classifier_version
        if contract is not None
        else str(raw_contract.get("classifier_version") or "")
    )
    if classifier_version != CLASSIFIER_VERSION:
        errors.append(f"automatic acceptance requires classifier {CLASSIFIER_VERSION}")

    ids = _catalog_ids(selection.get("catalog_ids"), errors)
    decisions, decision_by_id = _decision_ids(
        ids, selection.get("catalog_decisions"), errors
    )
    included_decision_ids = {
        int(decision["competition_id"])
        for decision in decisions
        if decision.get("decision") == "included"
    }
    if included_decision_ids != included_ids:
        errors.append(
            "catalog included decisions must exactly match contract included_ids"
        )
    for competition_id in sorted(included_ids):
        decision = decision_by_id.get(competition_id)
        if decision is None or not _has_structural_male_evidence(
            decision, classifier_version=classifier_version
        ):
            errors.append(
                f"included competition {competition_id} lacks successful structural male evidence"
            )

    planned = _planned_scopes(
        selection.get("planned_scopes"),
        included_ids=included_ids,
        decision_by_id=decision_by_id,
        errors=errors,
    )
    attempts = _scope_attempts(
        selection.get("scope_attempts"),
        contract_scopes=contract_scopes,
        plan_signature=plan_signature,
        planned_scopes=set(planned),
        lane=lane,
        now=checked_at,
        errors=errors,
    )
    if "\0soft" in attempts and status != "partial_success":
        errors.append("retryable or deferred scopes require partial_success status")

    budget = _mapping(report.get("budget"))
    if budget is None or type(budget.get("proxy_bytes")) is not int:
        errors.append("report budget.proxy_bytes must be an integer")
    elif budget.get("proxy_bytes") != 0:
        errors.append("proxy bytes must be zero")
    if budget is not None and "max_proxy_bytes" in budget:
        if type(budget.get("max_proxy_bytes")) is not int or budget.get("max_proxy_bytes") != 0:
            errors.append("proxy byte budget must be zero")

    return CatalogAcceptanceResult(
        ok=not errors,
        errors=tuple(errors),
        catalog_count=len(ids),
        included_count=len(included_ids),
        scope_count=len(contract_scopes),
    )


def _write_result(result: CatalogAcceptanceResult, output: str | None) -> None:
    rendered = json.dumps(result.as_dict(), ensure_ascii=False, sort_keys=True, indent=2)
    if output:
        Path(output).write_text(rendered + "\n", encoding="utf-8")
    else:
        print(rendered)


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description="Validate one fotmob-catalog-v1 automatic runner report",
        allow_abbrev=False,
    )
    parser.add_argument("--report", required=True, help="Runner JSON report path")
    parser.add_argument("--output", help="Optional acceptance JSON output path")
    args = parser.parse_args(argv)
    try:
        report = json.loads(Path(args.report).read_text(encoding="utf-8"))
        result = validate_report(report)
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        result = CatalogAcceptanceResult(
            ok=False,
            errors=(f"could not read runner report: {type(exc).__name__}: {exc}",),
            catalog_count=0,
            included_count=0,
            scope_count=0,
        )
    _write_result(result, args.output)
    return 0 if result.ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
