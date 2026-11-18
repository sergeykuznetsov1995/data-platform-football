ARG BASE_IMAGE
FROM ${BASE_IMAGE}

USER root

COPY scripts/research/clubelo_canary_solver_instrumentation.py /usr/local/libexec/clubelo-canary/solver_instrumentation.py
COPY docker/images/flaresolverr-canary/entrypoint.sh /usr/local/bin/clubelo-canary-entrypoint

RUN --network=none chown 0:0 /usr/local/libexec/clubelo-canary/solver_instrumentation.py /usr/local/bin/clubelo-canary-entrypoint \
    && chmod 0444 /usr/local/libexec/clubelo-canary/solver_instrumentation.py \
    && chmod 0555 /usr/local/bin/clubelo-canary-entrypoint \
    && install -d -o 1000 -g 1000 -m 0700 /var/lib/clubelo-canary-run \
    && if test -f /app/chromedriver && test ! -L /app/chromedriver; then \
        install -d -o 0 -g 0 -m 0755 /usr/local/share/clubelo-canary \
        && install -o 0 -g 0 -m 0444 /app/chromedriver /usr/local/share/clubelo-canary/chromedriver.original \
        && rm /app/chromedriver \
        && ln -s /tmp/clubelo-canary-chromedriver /app/chromedriver \
        || exit 1; \
    elif test -L /app/chromedriver \
        && test "$(readlink /app/chromedriver)" = /tmp/whoscored-chromedriver; then \
        :; \
    else \
        exit 1; \
    fi

ENTRYPOINT ["/usr/bin/dumb-init", "--"]
CMD ["/usr/local/bin/clubelo-canary-entrypoint"]

USER 1000:1000
