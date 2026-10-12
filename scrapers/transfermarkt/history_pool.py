"""Offline deployment preflight for the separately supplied historical exits."""
from __future__ import annotations

import json
import ipaddress
import re
from pathlib import Path
import sys


def validate_history_pool_files(current_file, history_file):
    """Do not report any credential value, even for malformed JSON."""
    def identities(path):
        try:
            raw = Path(path).read_text()
            if len(raw.encode()) > 1024 * 1024:
                raise ValueError
            def no_duplicates(pairs):
                result = {}
                for key, value in pairs:
                    if key in result:
                        raise ValueError
                    result[key] = value
                return result
            rows = json.loads(raw, object_pairs_hook=no_duplicates)
            if not isinstance(rows, list) or not rows or len(rows) > 1000:
                raise ValueError
            result = set()
            for row in rows:
                if not isinstance(row, dict) or set(row) != {'host', 'port', 'username', 'password'}:
                    raise ValueError
                if (isinstance(row['port'], bool) or not isinstance(row['port'], int)
                        or not 1 <= row['port'] <= 65535
                        or any(not isinstance(row[key], str) or not row[key].strip()
                               for key in ('host', 'username', 'password'))):
                    raise ValueError
                host = row['host']
                if host != host.strip() or len(host) > 253:
                    raise ValueError
                try:
                    host = ipaddress.ip_address(host).compressed
                except ValueError:
                    host = host.encode('idna').decode('ascii').lower()
                    if host.endswith('.') or any(not re.fullmatch(r'[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?', label)
                                                 for label in host.split('.')):
                        raise ValueError
                for key, limit in [('host', 253), ('username', 1024), ('password', 4096)]:
                    value = row[key]
                    value.encode('utf-8')
                    if len(value) > limit or any(ord(char) < 32 or ord(char) == 127 for char in value):
                        raise ValueError
                if ':' in row['username']:
                    raise ValueError
                result.add((host, row['port'], row['username']))
            if len(result) != len(rows):
                raise ValueError
            return result
        except (OSError, ValueError, TypeError) as exc:
            raise ValueError('invalid Transfermarkt proxy pool file') from None
    if identities(current_file) & identities(history_file):
        raise ValueError('historical exits overlap current exits')


if __name__ == '__main__':
    try:
        validate_history_pool_files(*sys.argv[1:])
    except (ValueError, TypeError) as exc:
        print(str(exc), file=sys.stderr)
        sys.exit(2)
