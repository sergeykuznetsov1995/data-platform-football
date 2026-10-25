#!/usr/bin/env python3
"""Validate eight frozen Bronze exports; optionally write an isolated Trino trial.

Run from repository root with PYTHONPATH=. . No .env loading, live source fetch,
current-Bronze fallback, production schema, DAG or scheduled execution.
"""

from __future__ import annotations
import argparse
import hashlib
import json
import os
from pathlib import Path

from scrapers.fotmob.silver_matches import load_contract, materialize
from scrapers.fotmob.silver_matches.inputs import load_input_set
from scrapers.fotmob.silver_matches.trino_trial import write_trial


def run(args):
    inputs = load_input_set(args.payloads, args.manifests)
    if len(inputs.inputs) != 8:
        raise ValueError("The bounded first release trial requires eight matches")
    if (
        args.expected_payload_sha256 != inputs.payload_csv_sha256
        or args.expected_manifest_sha256 != inputs.manifest_csv_sha256
    ):
        raise ValueError("Frozen input file hashes differ from expected receipts")
    if Path(args.output).exists():
        raise ValueError("Output receipt already exists")
    contract = load_contract()
    contract_hash = hashlib.sha256(
        json.dumps(
            {"schema": contract.schema, "registry": contract.registry},
            sort_keys=True,
            ensure_ascii=False,
        ).encode()
    ).hexdigest()
    root = Path(__file__).resolve().parents[2]
    code_files = sorted((root / "scrapers/fotmob/silver_matches").glob("*.py")) + [
        Path(__file__).resolve()
    ]
    code_hashes = {
        str(path.relative_to(root)): hashlib.sha256(path.read_bytes()).hexdigest()
        for path in code_files
    }
    transform_hash = hashlib.sha256(
        json.dumps(code_hashes, sort_keys=True).encode()
    ).hexdigest()
    publication = (
        "fm-match-"
        + hashlib.sha256(
            (inputs.fingerprint + contract_hash + transform_hash).encode()
        ).hexdigest()
    )
    result = materialize(inputs.records, publication_id=publication, contract=contract)
    receipt = {
        "mode": "validate_only",
        "input_set": inputs.receipt(),
        "contract_sha256": contract_hash,
        "transform_sha256": transform_hash,
        "code_sha256": code_hashes,
        "publication_id": publication,
        "tables": {t: len(rs) for t, rs in result.rows.items()},
        "diagnostics": list(result.diagnostics),
        "source": "frozen committed Bronze export",
        "scope": "First independent match release trial; no production activation",
    }
    if args.write:
        if not args.schema:
            raise ValueError("--schema required with --write")
        import trino
        import urllib3

        urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)
        user = os.environ.get("TRINO_USER", "airflow")
        password = os.environ.get("TRINO_PASSWORD")
        if not password:
            raise ValueError("Use the configured authenticated Trino environment")
        connection = trino.dbapi.connect(
            host=os.environ.get("TRINO_HOST", "trino"),
            port=int(os.environ.get("TRINO_PORT", "8443")),
            user=user,
            http_scheme="https",
            auth=trino.auth.BasicAuthentication(user, password),
            verify=False,
            session_properties={"query_max_run_time": "10m"},
        )
        try:
            receipt["iceberg_trial"] = write_trial(
                connection,
                schema_name=args.schema,
                result=result,
                input_set_id=inputs.fingerprint,
            )
        finally:
            connection.close()
        receipt["mode"] = "iceberg_readback_verified"
    with Path(args.output).open("x", encoding="utf-8") as stream:
        json.dump(receipt, stream, ensure_ascii=False, indent=2)
        stream.write("\n")
    print(
        json.dumps(
            {
                "mode": receipt["mode"],
                "matches": 8,
                "tables": len(result.rows),
                "output": args.output,
            },
            ensure_ascii=False,
        )
    )
    return receipt


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--payloads", required=True)
    p.add_argument("--manifests", required=True)
    p.add_argument("--expected-payload-sha256", required=True)
    p.add_argument("--expected-manifest-sha256", required=True)
    p.add_argument("--schema")
    p.add_argument("--write", action="store_true")
    p.add_argument("--output", required=True)
    run(p.parse_args())


if __name__ == "__main__":
    main()
