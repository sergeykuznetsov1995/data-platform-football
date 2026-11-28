#!/bin/sh
# Read-only Trino queries over ClubElo Bronze as src_clubelo (#2132).
#
# The ClubElo watchdogs used to read with the shared airflow principal through
# trino-ro.sh, so the ClubElo Trino meter could not tell their traffic from
# unattributed use of the shared account. Same monitor client and SQL gate;
# only the principal changes. The password never leaves the scheduler container.
exec /root/.claude/bin/trino-ro.sh --identity src_clubelo "$@"
