ARG BASE_IMAGE
FROM ${BASE_IMAGE}

USER root

COPY scripts/research/clubelo_canary_solver_instrumentation.py /usr/local/libexec/clubelo-canary/solver_instrumentation.py
COPY docker/images/flaresolverr-canary/entrypoint.sh /usr/local/bin/clubelo-canary-entrypoint

RUN --network=none chown 0:0 /usr/local/libexec/clubelo-canary/solver_instrumentation.py /usr/local/bin/clubelo-canary-entrypoint \
    && chmod 0444 /usr/local/libexec/clubelo-canary/solver_instrumentation.py \
    && chmod 0555 /usr/local/bin/clubelo-canary-entrypoint \
    && install -d -o 1000 -g 1000 -m 0700 /var/lib/clubelo-canary-run

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/local/bin/clubelo-canary-entrypoint"]

USER 1000:1000
