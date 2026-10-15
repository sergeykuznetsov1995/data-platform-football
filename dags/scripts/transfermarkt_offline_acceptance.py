#!/usr/bin/env python3
"""Offline D4 event exporter/owned-process harness; never starts acceptance."""
from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys

# Direct invocation from an isolated checkout, without application installation.
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
sys.path.insert(1, str(Path(__file__).resolve().parents[2] / 'dags'))
from scrapers.transfermarkt.offline_acceptance import InvalidEvidence, read_jsonl, resource_event, validate


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    check = commands.add_parser('validate', help='Read local JSONL; emit strict report to stdout')
    check.add_argument('input', type=Path)
    check.add_argument('--manifest-sha256', help='Externally pinned canonical manifest digest')
    check.add_argument('--peak-input', type=Path, help='Optional retained VmHWM/memory.peak records JSON list')
    demo = commands.add_parser('demo', help='Create synthetic intervals + owned process/file evidence')
    demo.add_argument('output_directory', type=Path)
    sample = commands.add_parser('resource', help='Export one resource event from retained local snapshots')
    sample.add_argument('input', type=Path, help='Pinned local JSONL event input')
    sample.add_argument('--window', required=True)
    sample.add_argument('--id', required=True)
    sample.add_argument('--sequence', type=int, required=True)
    sample.add_argument('--start', required=True)
    sample.add_argument('--end', required=True)
    sample.add_argument('--process-status', type=Path, required=True, help='Retained proc status text; VmRSS in kB')
    sample.add_argument('--process-identity', help='Retained boot/PID/start identity; absent means unlinked peaks')
    sample.add_argument('--cgroup-identity', help='Retained cgroup mount/device/inode identity')
    sample.add_argument('--cgroup-stats', type=Path, required=True, help='Retained memory.current/max/swap.current JSON')
    args = parser.parse_args(argv)
    if args.command == 'demo':
        from scrapers.transfermarkt.offline_harness import fixture, local_exercises, write_fixture
        args.output_directory.mkdir(parents=True, exist_ok=False)
        local = local_exercises(args.output_directory)
        (args.output_directory / 'owned-local-exercises.json').write_text(json.dumps(local, indent=2, sort_keys=True) + '\n')
        manifest, events = fixture()
        write_fixture(args.output_directory / 'synthetic-events.jsonl', manifest, events)
        report = validate(manifest, events)
        (args.output_directory / 'synthetic-validation.json').write_text(json.dumps(report, indent=2, sort_keys=True) + '\n')
    elif args.command == 'resource':
        try:
            manifest, events = read_jsonl(args.input)
            sample = resource_event(manifest, events, window_id=args.window, identity=args.id, sequence=args.sequence,
                                    start=args.start, end=args.end, process_status=args.process_status.read_text(),
                                    cgroup_stats=json.loads(args.cgroup_stats.read_text()),
                                    process_identity=args.process_identity, cgroup_identity=args.cgroup_identity)
            print(json.dumps(sample, sort_keys=True))
            return 0
        except (InvalidEvidence, OSError, ValueError, KeyError, TypeError) as exc:
            print(json.dumps({'artifact_status': 'INVALID', 'runtime_status': 'UNKNOWN', 'issues': [str(exc)]}))
            return 2
    else:
        try:
            manifest, events = read_jsonl(args.input)
            report = validate(manifest, events, expected_manifest_sha256=args.manifest_sha256,
                              peak_records=json.loads(args.peak_input.read_text()) if args.peak_input else None)
        except (InvalidEvidence, OSError, ValueError) as exc:
            report = {'artifact_status': 'INVALID', 'runtime_status': 'UNKNOWN', 'runtime_eligible': False,
                      'actual_acceptance_start': None, 'issues': [str(exc)]}
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0 if report['artifact_status'] == 'VALID' else 2


if __name__ == '__main__':
    raise SystemExit(main())
