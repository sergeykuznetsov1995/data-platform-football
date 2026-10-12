#!/usr/bin/env python3
"""Network-free FotMob typed-path projection matrix and completeness check."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from scrapers.fotmob.field_map import projection_matrix, validate_projections


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--check", action="store_true")
    parser.add_argument("--format", choices=("json", "markdown"), default="json")
    args = parser.parse_args(argv)
    matrix = projection_matrix()
    errors = validate_projections()
    if args.format == "json":
        print(json.dumps({"rows": matrix, "errors": errors}, ensure_ascii=False, indent=2))
    else:
        print("# FotMob: матрица проекций\n")
        print("Генерация: `python scripts/research/audit_fotmob_field_projection.py --check --format markdown`.\n")
        print("`typed` имеет конкретную колонку. Для остальных путей указана причина хранения raw или исключения.\n")
        print("| Цель | Путь | Статус | Колонка / причина |")
        print("|---|---|---|---|")
        for row in matrix:
            destination = ", ".join(row["columns"]) or row["reason"]
            cells = [row["target_type"], row["path"] or "(root)", row["disposition"], destination]
            print("| " + " | ".join(str(cell).replace("|", "\\|") for cell in cells) + " |")
    for error in errors:
        print(error, file=sys.stderr)
    return int(args.check and bool(errors))


if __name__ == "__main__":
    raise SystemExit(main())
