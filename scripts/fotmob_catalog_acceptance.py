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
from scrapers.fotmob.catalog import forbidden_competition_signal
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


def _has_structural_adult_male_source_evidence(
    decision: Mapping[str, Any], *, classifier_version: str
) -> bool:
    gender = str(decision.get("source_gender") or "").strip().casefold()
    age_group = str(decision.get("source_age_group") or "").strip().casefold()
    source_type = str(decision.get("source_type") or "").strip().casefold()
    content_hash = str(decision.get("profile_content_hash") or "")
    return bool(
        decision.get("probe_status") == "success"
        and decision.get("classifier_version") == classifier_version
        and gender in _MALE_GENDERS
        and (not age_group or age_group in _ADULT_AGE_GROUPS)
        and source_type in _KNOWN_TYPES
        and _nonempty_string(decision.get("profile_name"))
        and _nonempty_string(decision.get("profile_target_key"))
        and re.fullmatch(r"[0-9a-f]{64}", content_hash)
        and forbidden_competition_signal(
            decision.get("catalog_name"),
            decision.get("profile_name"),
            decision.get("source_gender"),
            decision.get("source_age_group"),
            decision.get("source_type"),
        )
        is None
    )


def _has_structural_male_evidence(
    decision: Mapping[str, Any], *, classifier_version: str
) -> bool:
    return bool(
        decision.get("decision") == "included"
        and decision.get("policy_rule") == "include_structural_male_adult"
        and _has_structural_adult_male_source_evidence(
            decision, classifier_version=classifier_version
        )
    )


def _forbidden_signal(decision: Mapping[str, Any]) -> str | None:
    return forbidden_competition_signal(
        *(
            decision.get(field)
            for field in (
                "catalog_name",
                "profile_name",
                "source_gender",
                "source_age_group",
                "source_type",
                "reason",
            )
        )
    )


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


def _deferrals(
    value: Any,
    *,
    contract_scopes: set[str],
    included_ids: set[int],
    errors: list[str],
) -> dict[str, dict[Any, Mapping[str, Any]]]:
    authorized: dict[str, dict[Any, Mapping[str, Any]]] = {
        "scope": {},
        "competition_discovery": {},
        "transfer": {},
    }
    if not isinstance(value, list):
        errors.append("selection.deferrals must be an array")
        return authorized
    for index, raw in enumerate(value):
        deferral = _mapping(raw)
        if deferral is None:
            errors.append(f"deferral #{index} must be an object")
            continue
        if set(deferral) != {"kind", "target_type", "targets", "reason"}:
            errors.append(f"deferral #{index} has an invalid evidence shape")
            continue
        kind = deferral.get("kind")
        target_type = deferral.get("target_type")
        reason = _nonempty_string(deferral.get("reason"))
        targets = deferral.get("targets")
        if kind not in {"budget", "deadline"}:
            errors.append(f"deferral #{index} must identify budget or deadline")
            continue
        if target_type not in authorized:
            errors.append(f"deferral #{index} has unknown target_type {target_type!r}")
            continue
        if reason is None or kind not in reason.casefold():
            errors.append(
                f"deferral #{index} reason must explicitly identify its {kind} cause"
            )
        if not isinstance(targets, list) or not targets:
            errors.append(f"deferral #{index} targets must be a non-empty array")
            continue
        parsed_targets: list[Any] = []
        for raw_target in targets:
            if target_type == "scope":
                try:
                    target = format_scope_token(*parse_scope_token(raw_target))
                except (TypeError, ValueError) as exc:
                    errors.append(f"deferral #{index} has invalid scope target: {exc}")
                    continue
                if target not in contract_scopes:
                    errors.append(
                        f"deferral #{index} scope {target!r} is outside the contract"
                    )
                    continue
            else:
                if type(raw_target) is not int or raw_target <= 0:
                    errors.append(
                        f"deferral #{index} competition targets must be positive integers"
                    )
                    continue
                target = raw_target
                if target not in included_ids:
                    errors.append(
                        f"deferral #{index} competition {target} is outside included IDs"
                    )
                    continue
            parsed_targets.append(target)
        if len(parsed_targets) != len(set(parsed_targets)):
            errors.append(f"deferral #{index} contains duplicate targets")
        for target in parsed_targets:
            if target in authorized[target_type]:
                errors.append(
                    f"deferral target {target!r} has duplicate {target_type} evidence"
                )
            else:
                authorized[target_type][target] = deferral
    return authorized


def _scope_attempts(
    value: Any,
    *,
    contract_scopes: set[str],
    plan_signature: str | None,
    planned_scopes: set[str],
    authorized_deferrals: Mapping[str, Mapping[str, Any]],
    lane: str | None,
    now: datetime,
    errors: list[str],
    require_full_completion: bool = True,
) -> tuple[dict[str, Mapping[str, Any]], bool]:
    if not isinstance(value, list):
        errors.append("selection.scope_attempts must be an array")
        return {}, False
    by_scope: dict[str, Mapping[str, Any]] = {}
    retryable_outcomes = False
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
        if outcome == "retryable":
            retryable_outcomes = True
            next_retry_at = _parse_timestamp(
                attempt.get("next_retry_at"),
                field=f"retryable scope {token!r} next_retry_at",
                errors=errors,
            )
            if reason is None:
                errors.append(f"retryable scope {token!r} must record an explicit reason")
            if require_full_completion:
                errors.append(
                    f"retryable scope {token!r} is incomplete and cannot be accepted"
                )
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
            # selection.scope_attempts — это перечитанная карта состояний по
            # подписи плана, то есть ИСТОРИЯ, а не работа этого рана. У кампании
            # история и есть предмет приёмки. У непрерывной полосы терминальный
            # исход прошлого рана уже покрасил ТОТ ран (гейт раннера считает
            # исходы своего рана), и повторно краснеть на нём вечно нельзя —
            # иначе одна разовая ошибка коммита навсегда останавливает silver.
            if require_full_completion:
                errors.append(f"terminal scope {token!r} is a hard failure")
            if reason is None:
                errors.append(f"terminal scope {token!r} must record an explicit reason")
        elif outcome == "deferred":
            next_retry_at = _parse_timestamp(
                attempt.get("next_retry_at"),
                field=f"deferred scope {token!r} next_retry_at",
                errors=errors,
            )
            if reason is None:
                errors.append(f"deferred scope {token!r} must record an explicit reason")
            evidence = authorized_deferrals.get(token)
            if evidence is None:
                errors.append(
                    f"deferred scope {token!r} lacks explicit budget/deadline evidence"
                )
            elif reason != evidence.get("reason"):
                errors.append(
                    f"deferred scope {token!r} reason differs from deferral evidence"
                )
            if (
                next_retry_at is not None
                and last_attempt_at is not None
                and next_retry_at <= last_attempt_at
            ):
                errors.append(
                    f"deferred scope {token!r} next_retry_at must follow its attempt"
                )

    missing_planned = sorted(planned_scopes.difference(by_scope))
    if missing_planned:
        errors.append(
            "every planned scope must have durable attempt evidence: "
            + ", ".join(missing_planned)
        )

    # Требование «каждый скоуп контракта закрыт терминально и свежее 72 часов»
    # — это вопрос кампании ко ВСЕМУ обязательству. Непрерывная полоса за одно
    # окно трогает десятки скоупов из ~450, поэтому к ней применима только
    # проверка выше: каждый ЗАПЛАНИРОВАННЫЙ в этом ране скоуп оставил
    # доказательство. Полноту покрытия у непрерывной полосы меряют по данным
    # (долг «сыграно без деталей»), а не по одному отчёту.
    if lane == "current" and require_full_completion:
        missing_current = sorted(contract_scopes.difference(by_scope))
        if missing_current:
            errors.append(
                "every current contract scope must have terminal completion evidence: "
                + ", ".join(missing_current)
            )
        for token in sorted(contract_scopes.intersection(by_scope)):
            attempt = by_scope[token]
            outcome = attempt.get("outcome")
            if outcome == "deferred" and token in authorized_deferrals:
                continue
            if outcome not in _TERMINAL_SCOPE_OUTCOMES:
                errors.append(f"current scope {token!r} is not terminal")
                continue
            last_attempt_at = _parse_timestamp(
                attempt.get("last_attempt_at"),
                field=f"current scope {token!r} last_attempt_at",
                errors=[],
            )
            if last_attempt_at is not None and now - last_attempt_at > CURRENT_COMPLETION_MAX_AGE:
                errors.append(f"current scope {token!r} completion is older than 72 hours")

    return by_scope, retryable_outcomes


def _transfer_completion(
    selection: Mapping[str, Any],
    *,
    contract: CatalogContract | None,
    included_ids: set[int],
    lane: str | None,
    status: Any,
    transfer_deferrals: Mapping[int, Mapping[str, Any]],
    errors: list[str],
) -> None:
    raw_completed = selection.get("completed_transfer_competition_ids")
    if not isinstance(raw_completed, list):
        errors.append(
            "selection.completed_transfer_competition_ids must be an array"
        )
        completed: list[int] = []
    else:
        completed = raw_completed
        if not all(type(value) is int and value > 0 for value in completed):
            errors.append(
                "transfer completion IDs must contain only positive integers"
            )
            completed = [
                value for value in completed if type(value) is int and value > 0
            ]
        if completed != sorted(set(completed)):
            errors.append("transfer completion IDs must be unique and sorted")

    contract_entities = set(contract.entities if contract is not None else ())
    transfer_policy = (
        contract.entity_policy.get("transfer_policy")
        if contract is not None
        else None
    )
    transfer_signature = selection.get("transfer_plan_signature")
    if "transfers" not in contract_entities:
        if completed:
            errors.append("transfer completion evidence exists outside entity policy")
        if transfer_signature is not None:
            errors.append("transfer plan signature must be null when transfers are absent")
        if transfer_policy is not None:
            errors.append("transfer policy exists while transfers are absent")
        if transfer_deferrals:
            errors.append("transfer deferrals exist while transfers are absent")
        return

    expected_policy = {
        "window": "1year" if lane == "current" else "all",
        "pagination": "unique_hits",
        "completion_scope": "included_ids",
        "completion_signature": "catalog_contract",
    }
    if transfer_policy != expected_policy:
        errors.append("catalog transfer completion policy is not exact")
    if contract is not None and transfer_signature != contract.plan_signature:
        errors.append("selection transfer plan signature differs from catalog contract")
    completed_set = set(completed)
    outside = sorted(completed_set - included_ids)
    if outside:
        errors.append(f"transfer completion IDs are outside included IDs: {outside}")
    missing = included_ids - completed_set
    if status == "success" and missing:
        errors.append(
            "transfer completion evidence is incomplete for included IDs: "
            + ", ".join(map(str, sorted(missing)))
        )
    if status == "partial_success":
        unexplained = missing - set(transfer_deferrals)
        if unexplained:
            errors.append(
                "missing transfer completion lacks budget/deadline deferral: "
                + ", ".join(map(str, sorted(unexplained)))
            )


def _validate_operation_retry_evidence(
    value: Any, *, errors: list[str], require_full_completion: bool = True
) -> None:
    if not isinstance(value, list):
        errors.append("runner report operations must be an array")
        return
    for index, raw in enumerate(value):
        operation = _mapping(raw)
        if operation is None:
            errors.append(f"operation #{index} must be an object")
            continue
        retryable = operation.get("retryable") or []
        if not isinstance(retryable, list):
            errors.append(f"operation #{index} retryable evidence must be an array")
            continue
        if not require_full_completion:
            # Непрерывная полоса: повтор на уровне операции — то же штатное
            # промежуточное состояние, что и retryable-скоуп. Структуру списка
            # проверяем всегда, содержимое трактуем как отказ только у кампании.
            continue
        for reason in retryable:
            text = str(reason).casefold()
            if not (
                ("budget" in text and ("request" in text or "byte" in text))
                or "deadline" in text
            ):
                errors.append(
                    f"operation #{index} has non-deferral retryable failure: {reason}"
                )


def validate_report(
    report: Mapping[str, Any],
    *,
    now: datetime | None = None,
    require_full_completion: bool = True,
) -> CatalogAcceptanceResult:
    """Validate one automatic runner report without performing any I/O.

    ``require_full_completion`` distinguishes two different questions asked of
    the same evidence.  A campaign or canary asks "is the whole obligation
    met?", so any scope still awaiting a retry invalidates the report.  A
    continuous lane asks "is this run's evidence honest?": it walks a ~450
    scope catalog under a time budget and can never close every scope in one
    window, so a scheduled retry is a normal intermediate state.  Structural
    checks (contract membership, signatures, timestamps, ordering, terminal
    outcomes) stay identical in both modes.
    """

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
    complete = report.get("complete")
    if not (
        (status == "success" and complete is True)
        or (status == "partial_success" and complete is False)
    ):
        errors.append("automatic report status/complete evidence is inconsistent")

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
    selection_signature = _nonempty_string(selection.get("scope_plan_signature"))
    if plan_signature is not None and selection_signature != plan_signature:
        errors.append(
            "selection.scope_plan_signature differs from recomputed catalog contract"
        )
    raw_entities = selection.get("entities")
    if (
        not isinstance(raw_entities, list)
        or not all(isinstance(value, str) and value for value in raw_entities)
        or raw_entities != sorted(set(raw_entities))
    ):
        errors.append("selection.entities must be canonical sorted strings")
    elif contract is not None and tuple(raw_entities) != contract.entities:
        errors.append("selection.entities differ from catalog contract entities")

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
    for decision in decisions:
        competition_id = int(decision["competition_id"])
        if (
            decision.get("decision") == "excluded"
            and _has_structural_adult_male_source_evidence(
                decision, classifier_version=classifier_version
            )
        ):
            errors.append(
                f"excluded competition {competition_id} has structural adult male evidence"
            )

    planned = _planned_scopes(
        selection.get("planned_scopes"),
        included_ids=included_ids,
        decision_by_id=decision_by_id,
        errors=errors,
    )
    deferrals = _deferrals(
        selection.get("deferrals"),
        contract_scopes=contract_scopes,
        included_ids=included_ids,
        errors=errors,
    )
    attempts, retryable_attempts = _scope_attempts(
        selection.get("scope_attempts"),
        contract_scopes=contract_scopes,
        plan_signature=plan_signature,
        planned_scopes=set(planned),
        authorized_deferrals=deferrals["scope"],
        lane=lane,
        now=checked_at,
        errors=errors,
        require_full_completion=require_full_completion,
    )
    deferral_count = sum(len(values) for values in deferrals.values())
    # У непрерывной полосы partial_success — ожидаемый исход почти каждого рана,
    # и доказательством неполноты служит сам факт незакрытого каталога, а не
    # обязательно бюджетная отсрочка. Требовать отсрочку значит красить ран,
    # который честно сказал «сделал часть».
    if status == "partial_success" and deferral_count == 0 and require_full_completion:
        errors.append(
            "partial_success requires explicit budget or deadline deferral evidence"
        )
    if status == "success" and deferral_count:
        errors.append("success cannot contain budget or deadline deferrals")
    # Здесь тоже история, а не работа рана: карта состояний хранит повторы,
    # назначенные прошлыми ранами (бэкофф до 24 ч). Требовать из-за них
    # incomplete у ТЕКУЩЕГО рана — значит краснеть за чужую работу; полноту
    # обхода у непрерывной полосы судит гейт раннера по своим исходам.
    if retryable_attempts and require_full_completion:
        errors.append("retryable scope evidence requires incomplete report status")

    _transfer_completion(
        selection,
        contract=contract,
        included_ids=included_ids,
        lane=lane,
        status=status,
        transfer_deferrals=deferrals["transfer"],
        errors=errors,
    )
    _validate_operation_retry_evidence(
        report.get("operations", []),
        errors=errors,
        require_full_completion=require_full_completion,
    )

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
