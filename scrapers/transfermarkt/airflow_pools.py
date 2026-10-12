"""The same stream settings size Airflow admission and the paid gateway."""

import argparse
import subprocess
from .streams import TransfermarktStreams


def pool_sizes(streams=None):
    streams = streams or TransfermarktStreams.from_env()
    return {'transfermarkt_control': streams.current_capacity,
            'transfermarkt_proxy': streams.current_capacity,
            'transfermarkt_backfill_control': streams.history_capacity,
            'transfermarkt_backfill_proxy': streams.history_capacity}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--apply', action='store_true')
    parser.add_argument('--get')
    args = parser.parse_args()
    sizes = pool_sizes()
    if args.get:
        print(sizes[args.get])
    elif args.apply:
        for name, slots in sizes.items():
            subprocess.run(['airflow', 'pools', 'set', name, str(slots), 'Dedicated Transfermarkt stream admission'], check=True)
    else:
        for name, slots in sizes.items():
            print(name, slots)


if __name__ == '__main__':
    main()
