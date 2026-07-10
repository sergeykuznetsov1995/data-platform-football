# Ноутбук аналитика: python + trino + pyiceberg + pandas.
# Спавнится JupyterHub'ом (DockerSpawner), лимит 2G/2cpu на юзера.
FROM quay.io/jupyter/minimal-notebook:python-3.12

RUN pip install --no-cache-dir \
    "trino[sqlalchemy]==0.336.0" \
    "pyiceberg[pyarrow]==0.9.1" \
    pandas \
    matplotlib \
    ipywidgets

# Стартовый пример-тур по gold-слою. /home/jovyan/work — персональный том юзера;
# docker инициализирует пустой том содержимым образа, поэтому новый аналитик
# видит ноутбук при первом спавне.
COPY --chown=1000:100 example_gold_tour.ipynb /home/jovyan/work/example_gold_tour.ipynb
