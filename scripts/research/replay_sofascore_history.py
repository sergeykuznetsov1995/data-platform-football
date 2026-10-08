#!/usr/bin/env python3
"""Offline-only controller rehearsal. Never connects to SofaScore or Bronze."""
from __future__ import annotations

import argparse
import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "dags"))
from scrapers.sofascore import history_controller  # noqa: E402
from scrapers.sofascore.workload_plan import load_static_workload_policy  # noqa: E402
from dags.utils.sofascore_all_mens_state import read_completed, read_failures, read_snapshot  # noqa: E402


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--snapshot', required=True)
    parser.add_argument('--inventory', required=True)
    parser.add_argument('--state', required=True)
    parser.add_argument('--failures')
    parser.add_argument('--checkpoint', required=True)
    parser.add_argument('--run-id', default='offline-rehearsal')
    parser.add_argument('--batch-size', type=int, default=3)
    parser.add_argument('--output', required=True)
    parser.add_argument('--finalize', action='store_true')
    args = parser.parse_args(argv)
    snapshot = read_snapshot(args.snapshot)
    inventory = json.loads(Path(args.inventory).read_text())
    campaign = snapshot['campaign_id']
    policy = load_static_workload_policy(ROOT / 'configs/sofascore/workload_policy.json')
    plan = history_controller.plan(
        snapshot, inventory=inventory, checkpoint_path=args.checkpoint,
        completed=read_completed(args.state, campaign_id=campaign),
        failures=read_failures(args.failures, campaign_id=campaign) if args.failures else {},
        authorized_season_classes=[name for name, budget in policy.classes.items() if budget.scope == 'season'],
        batch_size=args.batch_size, dag_run_id=args.run_id, release='offline-rehearsal',
    )
    report = history_controller.read_summary(args.checkpoint, campaign)
    if args.finalize:
        history_controller.finalize(args.checkpoint, campaign_id=campaign, run_id=args.run_id)
    evidence = {'source_request_count': 0, 'publication_count': 0, 'plan': plan,
                'groups': report['groups'], 'summary': report['summary'],
                'inputs': {str(p): hashlib.sha256(Path(p).read_bytes()).hexdigest()
                           for p in (args.snapshot, args.inventory, args.state)}}
    Path(args.output).write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
    print(evidence['summary'])
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
