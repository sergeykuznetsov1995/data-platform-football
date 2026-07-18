# JupyterHub для аналитиков (docs/design/analyst-access.md, фаза 5).
# Спавнит per-user контейнеры через docker.sock (DockerSpawner).
FROM quay.io/jupyterhub/jupyterhub:5.3@sha256:bc6fe7c7f2c9be15dab47cefc49b4ce42b17f2a1f7132b9a61c44feadf754ad9 AS python3.10-runtime

COPY requirements.lock /tmp/requirements.lock
RUN python -m pip install \
    --no-cache-dir --no-deps --require-hashes --only-binary=:all: \
    -r /tmp/requirements.lock
RUN --network=none python -m pip check \
    && python -c "import sys; raise SystemExit(sys.version_info[:2] != (3, 10))"
