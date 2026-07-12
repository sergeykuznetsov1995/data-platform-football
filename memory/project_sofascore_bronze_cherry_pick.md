---
name: sofascore-bronze-cherry-pick-2026-05-18
description: Superseded historical note for the pre-manifest SofaScore implementation
metadata:
  type: project
  status: superseded
---

# SofaScore Bronze cherry-pick — superseded

This note described the May 2026 standalone TLS/entity implementation. Those
paths, manual tournament maps, full partition rewrites and per-player event
requests are no longer production architecture.

Current sources of truth:

- `configs/sofascore/README.md` — registry discovery and activation;
- `configs/sofascore/endpoint_coverage.yaml` — endpoint/table/DQ coverage;
- `scrapers/sofascore/capture_engine.py` — endpoint manifest, budget, resume;
- `scrapers/sofascore/{pipeline,season_pipeline}.py` — raw replay plans;
- `dags/scripts/run_sofascore_scraper.py` — the single DAG/CLI/backfill runner.

The historical `_fetch_json_endpoint`, `_fetch_lineup_payload`, standalone
shot/stat/profile runners and manual ID instructions were intentionally deleted.
Use git history for the original benchmark narrative.
