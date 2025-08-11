#!/usr/bin/env python3
import json
import sys
from pathlib import Path

try:
    import jsonschema  # type: ignore
except Exception as exc:
    print("Install jsonschema: pip install jsonschema", file=sys.stderr)
    raise


def validate_records(schema_path: Path, records_path: Path) -> None:
    schema = json.loads(Path(schema_path).read_text())
    validator = jsonschema.Draft7Validator(schema)
    records = [json.loads(line) for line in Path(records_path).read_text().splitlines() if line.strip()]
    errors = []
    for idx, record in enumerate(records):
        for error in sorted(validator.iter_errors(record), key=str):
            errors.append(f"line {idx+1}: {error.message}")
    if errors:
        print("Invalid records:\n" + "\n".join(errors))
        sys.exit(1)
    print("OK: all records valid")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: validate_schema.py <schema.json> <records.ndjson>")
        sys.exit(2)
    validate_records(Path(sys.argv[1]), Path(sys.argv[2]))
