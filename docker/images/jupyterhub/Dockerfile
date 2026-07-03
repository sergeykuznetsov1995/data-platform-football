# JupyterHub для аналитиков (docs/design/analyst-access.md, фаза 5).
# Спавнит per-user контейнеры через docker.sock (DockerSpawner).
FROM quay.io/jupyterhub/jupyterhub:5.3

RUN pip install --no-cache-dir \
    oauthenticator==17.3.0 \
    dockerspawner==14.0.0 \
    jupyterhub-idle-culler==1.4.0
