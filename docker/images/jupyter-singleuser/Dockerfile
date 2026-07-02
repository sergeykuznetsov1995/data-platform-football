# Ноутбук аналитика: python + trino + pyiceberg + pandas.
# Спавнится JupyterHub'ом (DockerSpawner), лимит 2G/2cpu на юзера.
FROM quay.io/jupyter/minimal-notebook:python-3.12

RUN pip install --no-cache-dir \
    "trino[sqlalchemy]==0.336.0" \
    "pyiceberg[pyarrow]==0.9.1" \
    pandas \
    matplotlib \
    ipywidgets
