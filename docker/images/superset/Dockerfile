# Superset 4.1.2 — закрывает CVE-2025-27696 / CVE-2025-48912 (было 4.1.1)
# Драйвер Trino — только trino[sqlalchemy]; sqlalchemy-trino НЕ ставить.
FROM apache/superset:4.1.2@sha256:15e110b8533d3cb6a0d529512ea71252b0ac62e3f72b1f7a5000f1361822ac26 AS python3.10-runtime
USER root
# The lock is the exact wheel delta observed in the smoke-tested image. Redis
# and the remaining transitive closure are already bound by the base digest.
COPY requirements.lock /tmp/requirements.lock
RUN python -m pip install \
    --no-cache-dir --no-deps --require-hashes --only-binary=:all: \
    -r /tmp/requirements.lock
RUN --network=none python -m pip check \
    && python -c "import sys; raise SystemExit(sys.version_info[:2] != (3, 10))"
USER superset
