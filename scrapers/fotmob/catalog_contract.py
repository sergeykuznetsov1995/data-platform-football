"""Immutable contract for one automatically classified FotMob catalog plan."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from typing import Any, Iterable, Mapping

from .planner import deterministic_plan_signature
from .scope_codec import format_scope_token, parse_scope_token


CATALOG_CONTRACT_SCHEMA = "fotmob-catalog-v1"


def _sha256_lines(values: Iterable[str]) -> str:
    return hashlib.sha256(
        "".join(f"{value}\n" for value in values).encode("utf-8")
    ).hexdigest()


def _canonical_ids(values: Iterable[int]) -> tuple[int, ...]:
    raw = list(values)
    normalized: list[int] = []
    for value in raw:
        if type(value) is not int or value <= 0:
            raise ValueError("included IDs must be positive integers")
        normalized.append(value)
    if len(normalized) != len(set(normalized)):
        raise ValueError("included IDs contain a duplicate")
    return tuple(sorted(normalized))


def _canonical_scopes(
    values: Iterable[tuple[int, str] | str],
) -> tuple[str, ...]:
    parsed: list[tuple[int, str]] = []
    for value in values:
        scope = parse_scope_token(value) if isinstance(value, str) else tuple(value)
        if len(scope) != 2:
            raise ValueError("scope must contain competition ID and exact season")
        parsed.append((int(scope[0]), str(scope[1])))
    if len(parsed) != len(set(parsed)):
        raise ValueError("scopes contain a duplicate")
    return tuple(format_scope_token(*scope) for scope in sorted(parsed))


def _canonical_entities(values: Iterable[str]) -> tuple[str, ...]:
    normalized = tuple(
        sorted({str(value).strip().casefold() for value in values if str(value).strip()})
    )
    if not normalized:
        raise ValueError("at least one FotMob entity is required")
    return normalized


def _policy_json(value: Mapping[str, Any]) -> str:
    return json.dumps(
        dict(value), ensure_ascii=False, sort_keys=True, separators=(",", ":")
    )


@dataclass(frozen=True, slots=True)
class CatalogContract:
    """All evidence needed to reproduce an automatic catalog selection."""

    schema: str
    catalog_batch_id: str
    catalog_content_hash: str
    classifier_version: str
    parser_version: str
    entities: tuple[str, ...]
    entity_policy_json: str
    included_ids: tuple[int, ...]
    included_count: int
    included_ids_sha256: str
    scopes: tuple[str, ...]
    scope_count: int
    scope_sha256: str
    plan_signature: str

    @property
    def entity_policy(self) -> dict[str, Any]:
        return json.loads(self.entity_policy_json)

    def as_dict(self) -> dict[str, Any]:
        return {
            "schema": self.schema,
            "catalog_batch_id": self.catalog_batch_id,
            "catalog_content_hash": self.catalog_content_hash,
            "classifier_version": self.classifier_version,
            "parser_version": self.parser_version,
            "entities": list(self.entities),
            "entity_policy": self.entity_policy,
            "included_ids": list(self.included_ids),
            "included_count": self.included_count,
            "included_ids_sha256": self.included_ids_sha256,
            "scopes": list(self.scopes),
            "scope_count": self.scope_count,
            "scope_sha256": self.scope_sha256,
            "plan_signature": self.plan_signature,
        }


def build_catalog_contract(
    *,
    catalog_batch_id: str,
    catalog_content_hash: str,
    classifier_version: str,
    included_ids: Iterable[int],
    scopes: Iterable[tuple[int, str] | str],
    parser_version: str = "fotmob-native-v2",
    entities: Iterable[str] = ("season",),
    entity_policy: Mapping[str, Any] | None = None,
) -> CatalogContract:
    """Canonicalize and bind one exact catalog-derived coverage obligation."""

    scalar_fields = {
        "catalog_batch_id": catalog_batch_id,
        "catalog_content_hash": catalog_content_hash,
        "classifier_version": classifier_version,
        "parser_version": parser_version,
    }
    for name, value in scalar_fields.items():
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"{name} must not be empty")

    normalized_ids = _canonical_ids(included_ids)
    normalized_scopes = _canonical_scopes(scopes)
    scope_ids = {parse_scope_token(value)[0] for value in normalized_scopes}
    if not scope_ids.issubset(normalized_ids):
        raise ValueError("every exact scope must belong to an included ID")
    normalized_entities = _canonical_entities(entities)
    normalized_policy = dict(entity_policy or {})
    policy_json = _policy_json(normalized_policy)
    ids_sha = _sha256_lines(str(value) for value in normalized_ids)
    scope_sha = _sha256_lines(normalized_scopes)
    signature = deterministic_plan_signature(
        normalized_entities,
        {
            "classifier_version": classifier_version,
            "parser_version": parser_version,
            "entity_policy": normalized_policy,
            "included_ids_sha256": ids_sha,
            "scope_sha256": scope_sha,
        },
    )
    return CatalogContract(
        schema=CATALOG_CONTRACT_SCHEMA,
        catalog_batch_id=catalog_batch_id,
        catalog_content_hash=catalog_content_hash,
        classifier_version=classifier_version,
        parser_version=parser_version,
        entities=normalized_entities,
        entity_policy_json=policy_json,
        included_ids=normalized_ids,
        included_count=len(normalized_ids),
        included_ids_sha256=ids_sha,
        scopes=normalized_scopes,
        scope_count=len(normalized_scopes),
        scope_sha256=scope_sha,
        plan_signature=signature,
    )


def catalog_contract_from_dict(value: Mapping[str, Any]) -> CatalogContract:
    """Load a report contract only when every derived field recomputes exactly."""

    if not isinstance(value, Mapping):
        raise ValueError("catalog contract must be an object")
    if value.get("schema") != CATALOG_CONTRACT_SCHEMA:
        raise ValueError("schema must be fotmob-catalog-v1")
    raw_ids = value.get("included_ids")
    raw_scopes = value.get("scopes")
    raw_entities = value.get("entities")
    raw_policy = value.get("entity_policy")
    if (
        not isinstance(raw_ids, list)
        or not isinstance(raw_scopes, list)
        or not isinstance(raw_entities, list)
        or not isinstance(raw_policy, Mapping)
    ):
        raise ValueError(
            "included_ids, scopes, entities, and entity_policy have invalid types"
        )
    try:
        canonical_order = raw_ids == sorted(raw_ids) and raw_scopes == sorted(
            raw_scopes, key=parse_scope_token
        )
    except (TypeError, ValueError) as exc:
        raise ValueError("contract IDs and scopes are invalid") from exc
    if not canonical_order:
        raise ValueError("contract IDs and scopes must be canonical sorted values")
    rebuilt = build_catalog_contract(
        catalog_batch_id=value.get("catalog_batch_id"),
        catalog_content_hash=value.get("catalog_content_hash"),
        classifier_version=value.get("classifier_version"),
        parser_version=value.get("parser_version"),
        entities=raw_entities,
        entity_policy=raw_policy,
        included_ids=raw_ids,
        scopes=raw_scopes,
    )
    actual = rebuilt.as_dict()
    if set(value) != set(actual):
        raise ValueError("catalog contract fields do not match its schema")
    for field in actual:
        if field in {"included_count", "scope_count"} and type(value.get(field)) is not int:
            raise ValueError(f"catalog contract {field} does not recompute")
        if value.get(field) != actual[field]:
            raise ValueError(f"catalog contract {field} does not recompute")
    return rebuilt


validate_catalog_contract = catalog_contract_from_dict


__all__ = [
    "CATALOG_CONTRACT_SCHEMA",
    "CatalogContract",
    "build_catalog_contract",
    "catalog_contract_from_dict",
    "validate_catalog_contract",
]
