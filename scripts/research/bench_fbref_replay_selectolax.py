"""Micro-bench: Selectolax vs BeautifulSoup on real saved FBref pages.

Research probe for issue #969. Zero-network, zero paid bytes — reads the saved
FBref match fixtures and times the DOM-walk parsing operation three ways:
  1. BeautifulSoup + html.parser  (current production backend)
  2. BeautifulSoup + lxml
  3. selectolax (lexbor)

It reproduces the *shape* of the production parse (full parse + pull tables out
of HTML comments as FBref hides them there + walk every table/row/cell + read
cell text) WITHOUT rewriting the production parser. It reports per-engine time
AND an equivalence check (a speedup that finds fewer tables/rows/cells is not a
real win). FBref parsing is not on the ingest critical path (proxy-bound); the
one place this speed matters is the offline replay of ~18k saved pages
(dags/dag_replay_fbref.py).

Run with: /root/.venvs/dpf-pydoll/bin/python (has selectolax + bs4 + lxml).
"""

from __future__ import annotations

import argparse
import gzip
import json
import re
import statistics
import time
from pathlib import Path

_COMMENT_RE = re.compile(r"<!--(.*?)-->", re.DOTALL)


def _load_pages(html_dir: Path) -> list[str]:
    pages = [gzip.open(p, "rt").read() for p in sorted(html_dir.glob("*.html.gz"))]
    if not pages:
        raise SystemExit(f"No *.html.gz in {html_dir}")
    return pages


def _extract_bs4(html: str, backend: str) -> tuple[int, int, int, int]:
    from bs4 import BeautifulSoup, Comment

    def documents():
        root = BeautifulSoup(html, backend)
        yield root
        for comment in root.find_all(string=lambda v: isinstance(v, Comment)):
            markup = str(comment)
            if "<table" in markup.lower():
                yield BeautifulSoup(markup, backend)

    n_tables = n_rows = n_cells = checksum = 0
    for doc in documents():
        for table in doc.find_all("table"):
            n_tables += 1
            for row in table.find_all("tr"):
                cells = row.find_all(["th", "td"])
                if not cells:
                    continue
                n_rows += 1
                for cell in cells:
                    n_cells += 1
                    checksum += len(cell.get_text(strip=True))
    return n_tables, n_rows, n_cells, checksum


def _extract_selectolax(html: str) -> tuple[int, int, int, int]:
    from selectolax.parser import HTMLParser

    trees = [HTMLParser(html)]
    for markup in _COMMENT_RE.findall(html):
        if "<table" in markup.lower():
            trees.append(HTMLParser(markup))

    n_tables = n_rows = n_cells = checksum = 0
    for tree in trees:
        for table in tree.css("table"):
            n_tables += 1
            for row in table.css("tr"):
                cells = row.css("th, td")
                if not cells:
                    continue
                n_rows += 1
                for cell in cells:
                    n_cells += 1
                    checksum += len((cell.text(strip=True) or ""))
    return n_tables, n_rows, n_cells, checksum


ENGINES = {
    "bs4_html.parser": lambda h: _extract_bs4(h, "html.parser"),
    "bs4_lxml": lambda h: _extract_bs4(h, "lxml"),
    "selectolax": _extract_selectolax,
}


def _bench(pages: list[str], fn, iterations: int) -> tuple[float, tuple]:
    fn(pages[0])  # warm imports/caches
    durations = []
    summary = None
    for _ in range(iterations):
        t0 = time.perf_counter()
        agg = [0, 0, 0, 0]
        for html in pages:
            r = fn(html)
            for i in range(4):
                agg[i] += r[i]
        durations.append(time.perf_counter() - t0)
        summary = tuple(agg)
    return statistics.mean(durations), summary


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--html-dir", type=Path,
                    default=Path("tests/fixtures/fbref/matches"))
    ap.add_argument("--iterations", type=int, default=15)
    ap.add_argument("--output", type=Path)
    args = ap.parse_args()

    pages = _load_pages(args.html_dir)
    results = {}
    for name, fn in ENGINES.items():
        mean_s, summary = _bench(pages, fn, args.iterations)
        results[name] = {
            "mean_total_seconds": round(mean_s, 4),
            "mean_seconds_per_page": round(mean_s / len(pages), 5),
            "tables": summary[0], "data_rows": summary[1],
            "cells": summary[2], "text_checksum": summary[3],
        }

    base = results["bs4_html.parser"]["mean_seconds_per_page"]
    ref = (results["bs4_html.parser"]["tables"],
           results["bs4_html.parser"]["data_rows"],
           results["bs4_html.parser"]["cells"])
    for name, r in results.items():
        r["speedup_vs_html_parser"] = round(base / r["mean_seconds_per_page"], 2)
        r["structural_match"] = (
            (r["tables"], r["data_rows"], r["cells"]) == ref
        )

    report = {
        "pages": len(pages),
        "iterations": args.iterations,
        "engines": results,
        "note": (
            "FBref parsing is proxy-bound in normal ingest; parser speed only "
            "matters for the offline replay of ~18k saved pages. text_checksum "
            "may differ across engines due to whitespace handling — tables/"
            "data_rows/cells is the structural equivalence signal."
        ),
    }
    rendered = json.dumps(report, indent=2, ensure_ascii=False)
    if args.output:
        args.output.write_text(rendered + "\n")
    print(rendered)


if __name__ == "__main__":
    main()
