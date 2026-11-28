#!/usr/bin/env python3
"""Read-only CSV transport for legacy monitoring; installation is a separate step."""

import re
import subprocess
import sys

TIMEOUT_SECONDS = 870
# A source watchdog reads with its own account instead of the shared airflow
# principal, so the source's Trino meter can attribute that traffic (#2132).
# Only the name of the password variable crosses the host boundary; the value
# is read inside the scheduler container, exactly like TRINO_PASSWORD.
IDENTITIES = {"src_clubelo": "TRINO_SRC_CLUBELO_PASSWORD"}
# Execute outside the mounted application tree and never create bytecode there.
REMOTE_SCRIPT = r'''
import csv
import io
import os
import sys
import trino

try:
    user = os.environ.get("TRINO_MONITOR_IDENTITY")
    if user:
        password = os.environ.get(os.environ["TRINO_MONITOR_PASSWORD_ENV"])
        if not password:
            raise ValueError("source credential unavailable")
    else:
        password = os.environ.get("TRINO_PASSWORD")
        user = os.environ.get("TRINO_USER", "airflow")
    connection = trino.dbapi.connect(
        host=os.environ.get("TRINO_HOST", "trino"),
        port=int(os.environ.get("TRINO_PORT", 8443 if password else 8080)),
        user=user, catalog="iceberg",
        http_scheme="https" if password else "http", verify=False,
        auth=trino.auth.BasicAuthentication(user, password) if password else None,
        request_timeout=30, max_attempts=1,
        session_properties={"enable_dynamic_filtering": "false",
                            "query_max_run_time": "840s"},
    )
    cursor = connection.cursor()
    try:
        cursor.execute(sys.argv[1])
        rows = cursor.fetchall()
    finally:
        cursor.close()
        connection.close()
    if not rows:
        raise ValueError("empty result")
    output = io.StringIO(newline="")
    csv.writer(output, quoting=csv.QUOTE_ALL, lineterminator="\n").writerows(rows)
    sys.stdout.write(output.getvalue())
except Exception:
    print("trino-monitor: query failed or returned no rows", file=sys.stderr)
    sys.exit(1)
'''


def validate_sql(sql):
    """Fail closed on unsupported syntax, writes, and opaque connector functions."""
    from sqlglot import exp
    from sqlglot.dialects.trino import Trino

    # Trino SHOW is opaque in sqlglot. Accept only this complete, bounded
    # grammar used by monitor fallbacks, never a generic SHOW prefix.
    identifier = r"[A-Za-z_][A-Za-z0-9_]*"
    show_tables = (rf"\s*SHOW\s+TABLES\s+FROM\s+{identifier}\.{identifier}"
                   r"(?:\s+LIKE\s+'(?:[^']|'')*')?\s*;?\s*")
    if re.fullmatch(show_tables, sql, flags=re.IGNORECASE):
        return

    class StrictTrino(Trino):
        class Parser(Trino.Parser):
            def _parse_command(self):
                # The default fallback logs SQL before returning a Command node.
                raise ValueError("unsupported command")

    statements = StrictTrino().parse(sql)
    if len(statements) != 1 or not isinstance(
        statements[0], (exp.Select, exp.Union, exp.Intersect, exp.Except)
    ):
        raise ValueError("one SELECT query required")
    for node in statements[0].walk():
        if isinstance(node, (exp.DDL, exp.DML, exp.Command, exp.Into, exp.Lock)):
            raise ValueError("write or unsupported operation")
        # Unknown/custom functions can have side effects. Namespaced functions and
        # connector table functions can submit arbitrary SQL to another database.
        if isinstance(node, exp.Anonymous) and node.name.lower() != "to_iso8601":
            raise ValueError("unknown function")
        if isinstance(node, exp.Dot) and isinstance(node.expression, exp.Func):
            raise ValueError("namespaced function")
        if isinstance(node, exp.Table) and not isinstance(node.this, exp.Identifier):
            raise ValueError("table function")


def main(argv=None):
    argv = sys.argv[1:] if argv is None else argv
    identity = None
    if argv[:1] == ["--identity"]:
        if len(argv) != 3 or argv[1] not in IDENTITIES:
            print("usage: trino_monitor_ro.py [--identity NAME] SQL", file=sys.stderr)
            return 2
        identity, argv = argv[1], argv[2:]
    if len(argv) != 1:
        print("usage: trino_monitor_ro.py [--identity NAME] SQL", file=sys.stderr)
        return 2
    try:
        validate_sql(argv[0])
    except Exception:
        # Never echo SQL, parser exceptions, connection parameters, or credentials.
        print("trino-monitor: SQL rejected or SQL validator unavailable", file=sys.stderr)
        return 2
    options = ([] if identity is None else
               ["--env", "TRINO_MONITOR_IDENTITY=" + identity,
                "--env", "TRINO_MONITOR_PASSWORD_ENV=" + IDENTITIES[identity]])
    try:
        result = subprocess.run(
            ["docker", "exec", "--workdir", "/tmp", *options, "airflow-scheduler",
             "python", "-B", "-W", "ignore", "-c", REMOTE_SCRIPT, argv[0]],
            capture_output=True, text=True, timeout=TIMEOUT_SECONDS, check=False,
        )
    except (OSError, subprocess.TimeoutExpired):
        print("trino-monitor: transport unavailable or timed out", file=sys.stderr)
        return 1
    if result.returncode or not result.stdout.strip():
        print("trino-monitor: query failed or returned no rows", file=sys.stderr)
        return 1
    sys.stdout.write(result.stdout)
    return 0


if __name__ == "__main__":
    sys.exit(main())
