#!/usr/bin/env python3
"""Reviewed, one-off #1231 phantom season repair. No default database writes."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
import os
from pathlib import Path
import re
import sys
from typing import Any

if __package__ in (None, ""):
    sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from scrapers.fotmob.catalog import SEASONS_PARSER_VERSION, parse_seasons

TABLE = "fotmob_competition_seasons"
LIMITS = {337: 17, 335: 12, 274: 11, 230: 9}
FORMAT = "fotmob-1231-repair-v1"
SCALAR_TYPE = re.compile(
    r"(?:varchar(?:\(\d+\))?|char\(\d+\)|boolean|bigint|integer|smallint|tinyint|"
    r"double|real|decimal\(\d+,\s*\d+\)|date|timestamp(?:\(\d+\))?(?: with time zone)?)\Z"
)


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _digest(value: Any) -> str:
    return hashlib.sha256(_json(value).encode()).hexdigest()


def _ident(value: str) -> str:
    if not re.fullmatch(r"[a-zA-Z_][a-zA-Z_0-9]*", value):
        raise ValueError("invalid SQL identifier")
    return f'"{value}"'


def _literal(value: str | None) -> str:
    if value is None:
        return "NULL"
    if not isinstance(value, str):
        raise ValueError("backup cells must be strings or null")
    return "'" + value.replace("'", "''") + "'"


def _table(plan: dict, suffix: str = "") -> str:
    return ".".join(
        _ident(v) for v in (plan["catalog"], plan["schema"], TABLE + suffix)
    )


def _schema(client: Any, plan: dict) -> list[list[str]]:
    columns = [
        [row[0], row[1].lower()] for row in client.query(f"DESCRIBE {_table(plan)}")
    ]
    for name, kind in columns:
        _ident(name)
        if not SCALAR_TYPE.fullmatch(kind):
            raise ValueError(f"unsupported lossless backup type: {name} {kind}")
    required = {
        "competition_id",
        "source_season_key",
        "_target_batch_id",
        "_payload_sha256",
        "_raw_uri",
    }
    if not required.issubset({name for name, _ in columns}):
        raise ValueError("missing provenance columns")
    return columns


def _key_where(targets: list[dict]) -> str:
    return " OR ".join(
        "(CAST(competition_id AS VARCHAR) = "
        + _literal(str(t["competition_id"]))
        + " AND source_season_key = "
        + _literal(t["source_season_key"])
        + ")"
        for t in targets
    )


def _rows(client: Any, plan: dict) -> list[list[str | None]]:
    projection = ", ".join(
        f"CAST({_ident(name)} AS VARCHAR)" for name, _ in plan["columns"]
    )
    rows = client.query(
        f"SELECT {projection} FROM {_table(plan)} WHERE {_key_where(plan['targets'])}"
    )
    result = [list(row) for row in rows]
    for row in result:
        for cell in row:
            _literal(cell)
    return sorted(result, key=_json)


def _load_evidence(item: dict) -> dict:
    raw = Path(item["path"]).read_bytes()
    if hashlib.sha256(raw).hexdigest() != item["sha256"]:
        raise ValueError("raw evidence digest mismatch")
    if not item.get("raw_uri"):
        raise ValueError("raw evidence URI required")
    return {
        "sha256": item["sha256"],
        "raw_uri": item["raw_uri"],
        "raw": raw.decode("utf-8"),
    }


def _catalog_keys(evidence: dict, competition_id: int) -> tuple[dict, set[str]]:
    if hashlib.sha256(evidence["raw"].encode()).hexdigest() != evidence["sha256"]:
        raise ValueError("embedded raw evidence digest mismatch")
    payload = json.loads(evidence["raw"])
    if str(payload.get("details", {}).get("id")) != str(competition_id):
        raise ValueError("catalog competition identity mismatch")
    keys = {
        season.source_season_key for season in parse_seasons(payload, competition_id)
    }
    return payload, keys


def _validate_evidence(plan: dict) -> None:
    if plan["format"] != FORMAT or plan["parser_version"] != SEASONS_PARSER_VERSION:
        raise ValueError("repair format/parser version changed")
    targets = plan["targets"]
    counts = Counter(t["competition_id"] for t in targets)
    if not counts or any(k not in LIMITS or n > LIMITS[k] for k, n in counts.items()):
        raise ValueError("targets exceed reviewed #1231 competition bounds")
    identities = [(t["competition_id"], t["source_season_key"]) for t in targets]
    if len(set(identities)) != len(identities):
        raise ValueError("duplicate target key")
    for target in targets:
        cid, key = target["competition_id"], target["source_season_key"]
        if not isinstance(key, str) or not key or " - " in key:
            raise ValueError("expected exact bare phantom key")
        _, current = _catalog_keys(plan["catalogs"][str(cid)], cid)
        phases = {key + " - Apertura", key + " - Clausura"}
        if key in current or not current.intersection(phases):
            raise ValueError("current catalog does not prove phantom key")
        if not target["provenance"]:
            raise ValueError("exact batch provenance required")
        batches = [p["batch_id"] for p in target["provenance"]]
        if any(not isinstance(b, str) or not b for b in batches) or len(
            set(batches)
        ) != len(batches):
            raise ValueError("batch provenance must be unique and nonempty")
        for provenance in target["provenance"]:
            payload, parsed = _catalog_keys(provenance, cid)
            stats = payload.get("stats") or {}
            secondary = set(map(str, stats.get("seasonsWithLinks") or []))
            secondary.update(
                str(v["Name"])
                for v in stats.get("seasonStatLinks") or []
                if isinstance(v, dict) and v.get("Name") is not None
            )
            if key not in secondary or key in parsed or not parsed.intersection(phases):
                raise ValueError("historical raw does not prove phantom provenance")


def _check_current(client: Any, plan: dict) -> None:
    for cid, evidence in plan["catalogs"].items():
        rows = client.query(
            f"SELECT source_season_key, _payload_sha256, _raw_uri FROM {_table(plan, '_current')} "
            f"WHERE CAST(competition_id AS VARCHAR) = {_literal(cid)}"
        )
        available = {(r[0], r[1], r[2]) for r in rows}
        for target in plan["targets"]:
            if str(target["competition_id"]) != cid:
                continue
            if not any(
                (
                    target["source_season_key"] + " - " + phase,
                    evidence["sha256"],
                    evidence["raw_uri"],
                )
                in available
                for phase in ("Apertura", "Clausura")
            ):
                raise ValueError(
                    "current catalog evidence changed or is absent in current view"
                )


def _check_provenance(plan: dict) -> None:
    names = [name for name, _ in plan["columns"]]
    actual = set()
    for row in plan["rows"]:
        if len(row) != len(names):
            raise ValueError("backup row width changed")
        record = dict(zip(names, row))
        actual.add(
            (
                record["competition_id"],
                record["source_season_key"],
                record["_target_batch_id"],
                record["_payload_sha256"],
                record["_raw_uri"],
            )
        )
    expected = {
        (
            str(t["competition_id"]),
            t["source_season_key"],
            p["batch_id"],
            p["sha256"],
            p["raw_uri"],
        )
        for t in plan["targets"]
        for p in t["provenance"]
    }
    if expected != actual:
        raise ValueError("physical rows differ from exact reviewed provenance")


def make_plan(client: Any, manifest: dict, *, catalog: str, schema: str) -> dict:
    """Read-only: independently validate raw evidence and back up all physical versions."""
    if manifest.get("issue") != 1231:
        raise ValueError("expected reviewed #1231 manifest")
    catalogs = manifest["catalogs"]
    if len({c["competition_id"] for c in catalogs}) != len(catalogs):
        raise ValueError("duplicate catalog evidence")
    plan = {
        "format": FORMAT,
        "parser_version": SEASONS_PARSER_VERSION,
        "catalog": catalog,
        "schema": schema,
        "catalogs": {str(c["competition_id"]): _load_evidence(c) for c in catalogs},
        "targets": [
            {
                "competition_id": t["competition_id"],
                "source_season_key": t["source_season_key"],
                "provenance": [
                    {"batch_id": p["batch_id"], **_load_evidence(p)}
                    for p in t["provenance"]
                ],
            }
            for t in manifest["targets"]
        ],
    }
    if set(plan["catalogs"]) != {str(t["competition_id"]) for t in plan["targets"]}:
        raise ValueError("catalog evidence must cover exact target competitions")
    _validate_evidence(plan)
    plan["columns"] = _schema(client, plan)
    plan["rows"] = _rows(client, plan)
    _check_provenance(plan)
    _check_current(client, plan)
    return plan


def save_plan(path: Path, plan: dict) -> None:
    """Exclusive durable backup: refusal to overwrite is deliberate."""
    data = {**plan, "digest": _digest(plan)}
    with path.open("x", encoding="utf-8") as stream:
        os.chmod(path, 0o600)
        stream.write(_json(data) + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    directory_fd = os.open(path.parent, os.O_RDONLY)
    try:
        os.fsync(directory_fd)
    finally:
        os.close(directory_fd)


def execute(client: Any, path: Path, *, mode: str, writers_quiesced: bool) -> str:
    """One atomic Iceberg DML statement; repeat safely after uncertain client result."""
    if mode not in {"apply", "rollback"}:
        raise ValueError("explicit apply or rollback required")
    if not writers_quiesced:
        raise ValueError("writers must remain quiesced across validation and mutation")
    plan = json.loads(path.read_text())
    digest = plan.pop("digest")
    if _digest(plan) != digest:
        raise ValueError("backup digest mismatch")
    _validate_evidence(plan)
    _check_provenance(plan)
    if _schema(client, plan) != plan["columns"]:
        raise ValueError("physical schema changed")
    before = _rows(client, plan)
    if before and before != plan["rows"]:
        raise ValueError("target rows changed; refusing partial or unreviewed state")
    if mode == "apply":
        _check_current(client, plan)
        if not before:
            return "already_applied"
        # Full-row predicates protect every backed-up value; the external maintenance
        # window is still mandatory because Iceberg has no cross-query transaction.
        predicates = [
            "("
            + " AND ".join(
                f"CAST({_ident(name)} AS VARCHAR) IS NOT DISTINCT FROM {_literal(value)}"
                for (name, _), value in zip(plan["columns"], row)
            )
            + ")"
            for row in plan["rows"]
        ]
        client.query(f"DELETE FROM {_table(plan)} WHERE " + " OR ".join(predicates))
        if _rows(client, plan):
            raise ValueError(
                "apply postcondition failed; preserve backup and keep writers quiesced"
            )
        return "applied"
    if before:
        return "already_restored"
    columns = ", ".join(_ident(name) for name, _ in plan["columns"])
    values = ", ".join(
        "("
        + ", ".join(
            f"CAST({_literal(cell)} AS {kind})"
            for (_, kind), cell in zip(plan["columns"], row)
        )
        + ")"
        for row in plan["rows"]
    )
    client.query(f"INSERT INTO {_table(plan)} ({columns}) VALUES {values}")
    if _rows(client, plan) != plan["rows"]:
        raise ValueError(
            "rollback postcondition failed; preserve backup and keep writers quiesced"
        )
    return "restored"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("plan", "apply", "rollback"))
    parser.add_argument("--plan", required=True, type=Path)
    parser.add_argument("--manifest", type=Path)
    parser.add_argument("--catalog", default="iceberg")
    parser.add_argument("--schema", default="bronze")
    parser.add_argument(
        "--writers-quiesced",
        action="store_true",
        help="Assert externally verified exclusive maintenance window",
    )
    args = parser.parse_args(argv)
    if args.mode == "plan" and (not args.manifest or args.plan.exists()):
        parser.error("plan requires --manifest and a new --plan backup path")
    if args.mode != "plan" and not args.writers_quiesced:
        parser.error("apply/rollback require --writers-quiesced")
    from scripts.fotmob_acceptance import connect_from_env

    client = connect_from_env(catalog=args.catalog, schema=args.schema)
    try:
        if args.mode == "plan":
            result = make_plan(
                client,
                json.loads(args.manifest.read_text()),
                catalog=args.catalog,
                schema=args.schema,
            )
            save_plan(args.plan, result)
            print(
                _json(
                    {
                        "status": "planned",
                        "keys": len(result["targets"]),
                        "physical_rows": len(result["rows"]),
                        "plan": str(args.plan),
                    }
                )
            )
        else:
            print(
                _json(
                    {
                        "status": execute(
                            client,
                            args.plan,
                            mode=args.mode,
                            writers_quiesced=args.writers_quiesced,
                        )
                    }
                )
            )
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
