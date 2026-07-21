FROM apache/airflow:2.11.2-python3.11@sha256:aac8468234dfa802c5ac4d6318bbf3b620793b55569ac230f584dc3c1f0ca49d AS airflow-base

USER root

# Bind every Debian package lookup below to one immutable bookworm snapshot.
# The detached Release receipt is checked independently before APT consumes the
# matching timestamped source URL.
RUN curl -fsSL https://snapshot.debian.org/archive/debian/20260223T000000Z/dists/bookworm/Release -o /tmp/debian-bookworm-Release \
    && echo "b03f3e7ae9364698af1a3980d2be386c3ce5f476682b5199c4999c514df9f103  /tmp/debian-bookworm-Release" \
        | sha256sum -c - \
    && test "$(stat -c %s /tmp/debian-bookworm-Release)" -eq 149269

# Install Java and system dependencies
RUN rm -rf /etc/apt/sources.list.d /var/lib/apt/lists/* \
    && printf '%s\n' 'deb https://snapshot.debian.org/archive/debian/20260223T000000Z bookworm main' >/etc/apt/sources.list \
    && APT_CONFIG=/dev/null apt-get -o Dir::Etc::sourcelist=/etc/apt/sources.list -o Dir::Etc::sourceparts=- -o Dir::Etc::main=- -o Dir::Etc::parts=- -o Dir::State::lists=/var/lib/apt/lists update \
    && APT_CONFIG=/dev/null apt-get -o Dir::Etc::sourcelist=/etc/apt/sources.list -o Dir::Etc::sourceparts=- -o Dir::Etc::main=- -o Dir::Etc::parts=- -o Dir::State::lists=/var/lib/apt/lists install -y --no-install-recommends \
    openjdk-17-jdk-headless=17.0.17+10-1~deb12u1 \
    procps=2:4.0.2-3 \
    curl=7.88.1-10+deb12u14 \
    netcat-openbsd=1.219-1 \
    git=1:2.39.5-0+deb12u3 \
    && rm -rf /var/lib/apt/lists/*

# Chrome/Chromium for Selenium (headless browser automation)
# Xvfb for virtual display (bypasses Cloudflare headless detection)
# Mesa software-GL (libgl1-mesa-dri/glx/egl): gives Chromium 120 a real (non-null)
# WebGL context under Xvfb — Cloudflare reads a null WebGL context as a bot marker (#574)
RUN rm -rf /etc/apt/sources.list.d /var/lib/apt/lists/* \
    && printf '%s\n' 'deb https://snapshot.debian.org/archive/debian/20260223T000000Z bookworm main' >/etc/apt/sources.list \
    && APT_CONFIG=/dev/null apt-get -o Dir::Etc::sourcelist=/etc/apt/sources.list -o Dir::Etc::sourceparts=- -o Dir::Etc::main=- -o Dir::Etc::parts=- -o Dir::State::lists=/var/lib/apt/lists update \
    && APT_CONFIG=/dev/null apt-get -o Dir::Etc::sourcelist=/etc/apt/sources.list -o Dir::Etc::sourceparts=- -o Dir::Etc::main=- -o Dir::Etc::parts=- -o Dir::State::lists=/var/lib/apt/lists install -y --no-install-recommends \
    chromium=143.0.7499.169-1~deb12u1 \
    chromium-driver=143.0.7499.169-1~deb12u1 \
    libgl1-mesa-dri=22.3.6-1+deb12u1 \
    libgl1-mesa-glx=22.3.6-1+deb12u1 \
    libegl1-mesa=22.3.6-1+deb12u1 \
    fonts-liberation=1:1.07.4-11 \
    libasound2=1.2.8-1+b1 \
    libatk-bridge2.0-0=2.46.0-5 \
    libdrm2=2.4.114-1+b1 \
    libgbm1=22.3.6-1+deb12u1 \
    libnspr4=2:4.35-1 \
    libnss3=2:3.87.1-1+deb12u1 \
    libxkbcommon0=1.5.0-1 \
    xdg-utils=1.1.3-4.1 \
    xvfb=2:21.1.7-3+deb12u11 \
    xauth=1:1.1.2-1 \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
# Force software GL (llvmpipe) — no GPU in the container; avoids failed HW-GL probes (#574)
ENV LIBGL_ALWAYS_SOFTWARE=1

# Set Java environment
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Install Spark
ENV SPARK_VERSION=3.5.3
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark

RUN curl --proto '=https' --tlsv1.2 --proto-redir '=https' \
      --connect-timeout 20 --speed-limit 1024 --speed-time 60 \
      --max-time 3600 -fsSL \
      https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3-bin-hadoop3.tgz -o /tmp/spark-3.5.3-bin-hadoop3.tgz \
    && echo "173651a8a00f5bf0ee27b74d817e0e52eed9daa49fd66d59718994974d1d367d  /tmp/spark-3.5.3-bin-hadoop3.tgz" \
        | sha256sum -c - \
    && test "$(stat -c %s /tmp/spark-3.5.3-bin-hadoop3.tgz)" -eq 400864419
RUN --network=none tar -xzf /tmp/spark-3.5.3-bin-hadoop3.tgz -C /opt/ \
    && mv /opt/spark-3.5.3-bin-hadoop3 /opt/spark \
    && chmod -R 755 /opt/spark \
    && rm -f /tmp/spark-3.5.3-bin-hadoop3.tgz

ENV PATH=$SPARK_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Download PostgreSQL JDBC driver for Spark
RUN curl -fsSL https://jdbc.postgresql.org/download/postgresql-42.7.4.jar -o /opt/spark/jars/postgresql-42.7.4.jar \
    && echo "188976721ead8e8627eb6d8389d500dccc0c9bebd885268a3047180274a6031e  /opt/spark/jars/postgresql-42.7.4.jar" \
        | sha256sum -c - \
    && test "$(stat -c %s /opt/spark/jars/postgresql-42.7.4.jar)" -eq 1086687

# Iceberg Spark runtime for PySpark jobs
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.4.3/iceberg-spark-runtime-3.5_2.12-1.4.3.jar -o /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar \
    && echo "bbb6b493283280a348e1f1831a348f6128c2e7c33a7646b66974e0b0503fac5c  /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar" \
        | sha256sum -c - \
    && test "$(stat -c %s /opt/spark/jars/iceberg-spark-runtime-3.5_2.12-1.4.3.jar)" -eq 29105653

# AWS bundle for S3 compatibility
RUN curl -fsSL https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/1.4.3/iceberg-aws-bundle-1.4.3.jar -o /opt/spark/jars/iceberg-aws-bundle-1.4.3.jar \
    && echo "172b776e93473b9f7bbacc41687e066befb5871c783dbd4426690cd4b17541f6  /opt/spark/jars/iceberg-aws-bundle-1.4.3.jar" \
        | sha256sum -c - \
    && test "$(stat -c %s /opt/spark/jars/iceberg-aws-bundle-1.4.3.jar)" -eq 28909126

# Create directories.  The soccerdata path must already belong to the numeric
# Airflow user in this base stage: Docker seeds a fresh named volume from the
# first image that mounts it, before the scheduler-only stage can fix ownership.
RUN --network=none mkdir -p /opt/airflow/spark_jobs /opt/airflow/dags /opt/airflow/logs /opt/airflow/scrapers \
    && install -d -o 50000 -g 0 -m 0755 /home/airflow/soccerdata

# Install the root-owned WhoScored trust implementation and image-owned trust
# root now, but do not activate its site hooks until the common dependency
# install below is complete. Source distributions may start an ordinary child
# interpreter even when pip itself was launched with ``-S``; activating the
# fail-closed runtime before the application closure exists would kill that
# build-time child with exit 78.
COPY whoscored_runtime_startup.py /usr/local/lib/whoscored_runtime_startup.py
COPY whoscored_runtime_pth.py /usr/local/lib/python3.11/site-packages/whoscored_runtime_pth.py
RUN --network=none install -d -o root -g root -m 0755 /usr/local/share/whoscored \
    && install -o root -g root -m 0444 /dev/null \
      /usr/local/share/whoscored/airflow.cfg
COPY --chown=root:root whoscored-runtime-trust-root-generic /usr/local/share/whoscored/runtime-trust-root
RUN --network=none chmod 0444 \
      /usr/local/lib/whoscored_runtime_startup.py \
      /usr/local/lib/python3.11/site-packages/whoscored_runtime_pth.py \
      /usr/local/share/whoscored/airflow.cfg \
      /usr/local/share/whoscored/runtime-trust-root \
    && test "$(stat -c '%u:%g:%a:%s' /usr/local/share/whoscored/airflow.cfg)" \
      = "0:0:444:0"

# Keep the attested application package root ahead of Airflow's bare ``utils``
# compatibility path.  Adding ``/opt/airflow/scrapers`` exposes unintended
# top-level packages, while putting ``dags`` first lets dags/scripts shadow the
# canonical root scripts package.
ENV AIRFLOW_CONFIG=/usr/local/share/whoscored/airflow.cfg
ENV PYTHONPATH="" \
    LD_LIBRARY_PATH="" \
    PYTHONNOUSERSITE=1 \
    PYTHONSAFEPATH=1

# Gunicorn 25.1 enables a control socket by default and otherwise creates
# gunicorn.ctl in AIRFLOW_HOME after the attested runtime tree is pinned.
# Airflow does not use that new control interface, so keep the release tree
# immutable and retain the existing worker tmp directory setting.
ENV GUNICORN_CMD_ARGS="--worker-tmp-dir /dev/shm --no-control-socket"

# Switch to airflow user for pip installs
USER airflow

# Immutable inputs for SofaScore paid-canary provenance.  Application source
# and configs are bind-mounted in production, but build/dependency inputs are
# not; bake exact readable copies so the runtime loader can reject a stale
# image whose installed browser stack no longer matches the checked-in pins.
COPY --chown=root:root Dockerfile /opt/airflow/runtime-contract/docker/images/airflow/Dockerfile
COPY --chown=root:root requirements-airflow.txt /opt/airflow/runtime-contract/docker/images/airflow/requirements-airflow.txt
COPY --chown=root:root requirements-build-tools.txt /opt/airflow/runtime-contract/docker/images/airflow/requirements-build-tools.txt
COPY --chown=root:root requirements-scraping.txt /opt/airflow/runtime-contract/docker/images/airflow/requirements-scraping.txt
COPY --chown=root:root requirements-scraper-runner.txt /opt/airflow/runtime-contract/docker/images/airflow/requirements-scraper-runner.txt
COPY --chown=root:root requirements-scheduler.txt /opt/airflow/runtime-contract/docker/images/airflow/requirements-scheduler.txt
COPY --chown=root:root requirements.txt /opt/airflow/runtime-contract/docker/images/airflow/requirements.txt
COPY --chown=root:root whoscored-production-entrypoint /opt/airflow/runtime-contract/docker/images/airflow/whoscored-production-entrypoint
COPY --chown=root:root whoscored-production-gate /opt/airflow/runtime-contract/docker/images/airflow/whoscored-production-gate
COPY --chown=root:root whoscored-production-python /opt/airflow/runtime-contract/docker/images/airflow/whoscored-production-python
COPY --chown=root:root whoscored_production_gate.py /opt/airflow/runtime-contract/docker/images/airflow/whoscored_production_gate.py
COPY --chown=root:root whoscored_capacity_worker_bootstrap.py /opt/airflow/runtime-contract/docker/images/airflow/whoscored_capacity_worker_bootstrap.py
COPY --chown=root:root whoscored_runtime_pth.py /opt/airflow/runtime-contract/docker/images/airflow/whoscored_runtime_pth.py
COPY --chown=root:root whoscored_runtime_startup.py /opt/airflow/runtime-contract/docker/images/airflow/whoscored_runtime_startup.py
COPY --chown=root:root whoscored-flaresolverr-runtime.root.dockerignore /opt/airflow/runtime-contract/.dockerignore
COPY --chown=root:root whoscored-flaresolverr-runtime.Dockerfile /opt/airflow/runtime-contract/docker/images/flaresolverr-whoscored/Dockerfile
COPY --chown=root:root whoscored-flaresolverr-runtime.Dockerfile.dockerignore /opt/airflow/runtime-contract/docker/images/flaresolverr-whoscored/Dockerfile.dockerignore
COPY --chown=root:root whoscored-flaresolverr-runtime.entrypoint.sh /opt/airflow/runtime-contract/docker/images/flaresolverr-whoscored/entrypoint.sh

# ===== СТАДИЯ 1: Airflow 2.11.2 (уже включен в базовый образ) =====
# Базовый образ apache/airflow:2.11.2-python3.11 уже содержит Airflow
# Дополнительная установка Airflow не требуется

# ===== СТАДИЯ 2: Core dependencies (Airflow providers, data processing) =====
# The pinned base ships system setuptools 79.0.1 while its preinstalled
# Snowflake provider requires >=80. Keep this build tool in the system prefix:
# a user-site copy would remain shadowed by the trusted system path.
USER root
COPY requirements-build-tools.txt /tmp/requirements-build-tools.txt
RUN PYTHONPATH=/usr/local/lib/python3.11/site-packages:/home/airflow/.local/lib/python3.11/site-packages \
    /usr/local/bin/python -S -m pip install \
      --no-cache-dir --no-deps --require-hashes --only-binary=:all: \
      -r /tmp/requirements-build-tools.txt
RUN --network=none SOURCE_DATE_EPOCH=1725840000 PYTHONHASHSEED=0 \
    PYTHONPATH=/usr/local/lib/python3.11/site-packages:/home/airflow/.local/lib/python3.11/site-packages \
    /usr/local/bin/python -S -m pip wheel \
      --disable-pip-version-check --no-deps --no-build-isolation \
      --wheel-dir /tmp/whoscored-airflow-wheelhouse \
      /opt/spark/python \
    && echo "db93d7da4bc3ecee35ca2389d1352a4173c6b8aaf6f7d1bd9049131dbf174e5f  /tmp/whoscored-airflow-wheelhouse/pyspark-3.5.3-py2.py3-none-any.whl" \
        | sha256sum -c - \
    && test "$(stat -c %s /tmp/whoscored-airflow-wheelhouse/pyspark-3.5.3-py2.py3-none-any.whl)" -eq 369803427
USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN PIP_FIND_LINKS=/tmp/whoscored-airflow-wheelhouse \
    PYTHONUSERBASE=/home/airflow/.local \
    PYTHONPATH=/usr/local/lib/python3.11/site-packages:/home/airflow/.local/lib/python3.11/site-packages \
    /usr/local/bin/python -S -m pip install \
      --no-cache-dir --user --no-deps --require-hashes --only-binary=:all: \
      -r /tmp/requirements.txt

# Keep the conflicting browser dependency set out of the Airflow interpreter.
# The scheduler target installs it in an isolated subprocess venv below.
COPY requirements-scraping.txt /tmp/requirements-scraping.txt
COPY requirements-scraper-runner.txt /tmp/requirements-scraper-runner.txt
COPY requirements-scheduler.txt /tmp/requirements-scheduler.txt
USER root
RUN --network=none rm -rf /tmp/whoscored-airflow-wheelhouse
USER airflow
RUN --network=none PYTHONPATH=/usr/local/lib/python3.11/site-packages:/home/airflow/.local/lib/python3.11/site-packages \
    /usr/local/bin/python -S -m pip check

# Airflow 2.11's embedded task-log server bypasses GUNICORN_CMD_ARGS. Gunicorn
# 25.1 therefore creates a new control socket in AIRFLOW_HOME unless the
# standalone application opts out explicitly. Patch only the exact upstream
# file and its wheel RECORD entry, and fail the build if any reviewed byte
# differs on either side of the patch.
USER root
RUN --network=none echo \
      "e1ab5ba5f4c4f02313e9559e113dfbcdbf0c7ed91aae48ea69c799c57623e0a4  /home/airflow/.local/lib/python3.11/site-packages/airflow/utils/serve_logs.py" \
      | sha256sum -c - \
    && echo \
      "fb02e1fae51fdcf4e1d8f5b232f3608ec0bcd88247d48da55eacb91ef5b107a4  /home/airflow/.local/lib/python3.11/site-packages/apache_airflow-2.11.2.dist-info/RECORD" \
      | sha256sum -c - \
    && sed -i \
      's@    options = \[bind_option, GunicornOption("workers", 2)\]@    options = [bind_option, GunicornOption("workers", 2), GunicornOption("control_socket_disable", True)]@' \
      /home/airflow/.local/lib/python3.11/site-packages/airflow/utils/serve_logs.py \
    && sed -i \
      's@airflow/utils/serve_logs.py,sha256=4atbpfTE8CMT6VWeET37zb8MftkarkjqaceZxXYj4KQ,7185@airflow/utils/serve_logs.py,sha256=NwdZuAeO3QAkcKDd-w4wMDZaCEWfKESz4QI2f_oWbis,7233@' \
      /home/airflow/.local/lib/python3.11/site-packages/apache_airflow-2.11.2.dist-info/RECORD \
    && echo \
      "370759b8078edd002470a0ddfb0e3030365a08459f2844b3e102367ffa166e2b  /home/airflow/.local/lib/python3.11/site-packages/airflow/utils/serve_logs.py" \
      | sha256sum -c - \
    && echo \
      "cbeb35f3979408ad5997cb5bc10f9733a7d4b42d2d28174126536271f2fbe083  /home/airflow/.local/lib/python3.11/site-packages/apache_airflow-2.11.2.dist-info/RECORD" \
      | sha256sum -c -

# Generic services may import the image dependency tree but cannot modify it
# or replace it through a writable parent directory.
RUN --network=none chown -R root:root /home/airflow/.local \
    && chown root:root /home/airflow \
    && chmod 0755 /home/airflow \
    && chmod -R u=rwX,go=rX /home/airflow/.local
USER airflow

# Set working directory
WORKDIR /opt/airflow

# Repository bytecode is deliberately unusable in every runtime target.  The
# prefix is an absent root-owned path; with writes disabled Python can neither
# consume a host-provided __pycache__ nor create a cache shared by later tasks.
ENV PYTHONPYCACHEPREFIX=/__whoscored_runtime_bytecode_disabled__ \
    PYTHONDONTWRITEBYTECODE=1

# Activate the trust anchor only after every common dependency install and
# ownership freeze. The two hooks bracket every dependency-owned .pth, and no
# later command in this base stage starts Python.
USER root
RUN --network=none printf '%s\n' \
      'import sys;exec("try:\n _p=\"/usr/local/lib/python3.11/site-packages/whoscored_runtime_pth.py\"\n _n={\"__builtins__\":__builtins__}\n exec(compile(open(_p,\"rb\").read(),_p,\"exec\"),_n)\n _n[\"run\"](\"bootstrap\")\nexcept BaseException:\n sys.modules[\"posix\"]._exit(78)",{"sys":sys})' \
      > /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth \
    && printf '%s\n' \
      'import sys;exec("try:\n _p=\"/usr/local/lib/python3.11/site-packages/whoscored_runtime_pth.py\"\n _n={\"__builtins__\":__builtins__}\n exec(compile(open(_p,\"rb\").read(),_p,\"exec\"),_n)\n _n[\"run\"](\"finalize\")\nexcept BaseException:\n sys.modules[\"posix\"]._exit(78)",{"sys":sys})' \
      > /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth \
    && chmod 0444 \
      /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth \
      /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth \
    && test "$(stat -c '%u:%g:%a:%h' /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth)" = "0:0:444:1" \
    && test "$(stat -c '%u:%g:%a:%h' /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth)" = "0:0:444:1" \
    && test "$(find /usr/local/lib/python3.11/site-packages -maxdepth 1 \
        -type f -name '*.pth' -printf '%f\n' | LC_ALL=C sort | head -n 1)" \
      = "00000000-whoscored-runtime-bootstrap.pth" \
    && test "$(find /usr/local/lib/python3.11/site-packages -maxdepth 1 \
        -type f -name '*.pth' -printf '%f\n' | LC_ALL=C sort | tail -n 1)" \
      = "zzzzzzzz-whoscored-runtime-finalize.pth"
USER airflow

# Scheduler-only native TLS layer. It is a target in this same build graph, so
# a cold-cache scheduler build never depends on a locally pre-tagged sibling
# image and still shares the exact base stages above.
FROM airflow-base AS airflow-scheduler-payload

USER root
# One-shot capacity workers run this image-owned coordinator under ``-I -S``.
# Only its child production interpreter activates the attested image site.
COPY --chown=root:root whoscored_capacity_worker_bootstrap.py /usr/local/libexec/whoscored_capacity_worker_bootstrap.py
RUN --network=none chmod 0444 /usr/local/libexec/whoscored_capacity_worker_bootstrap.py \
    && test "$(stat -c '%u:%g:%a:%h' /usr/local/libexec/whoscored_capacity_worker_bootstrap.py)" = "0:0:444:1"
# The generic parent is already runtime-sealed. Keep its root-owned hooks out
# of site-packages only for this build stage while scheduler-only dependencies
# are installed: pip source builds can start a child interpreter without ``-S``.
# The final payload restores and rechecks the exact two files before it can be
# used by any runtime target.
RUN --network=none test "$(stat -c '%u:%g:%a:%h' /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth)" = "0:0:444:1" \
    && test "$(stat -c '%u:%g:%a:%h' /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth)" = "0:0:444:1" \
    && test ! -e /usr/local/share/whoscored/build-hooks \
    && install -d -o root -g root -m 0755 /usr/local/share/whoscored/build-hooks \
    && test ! -e /usr/local/share/whoscored/build-hooks/bootstrap \
    && test ! -e /usr/local/share/whoscored/build-hooks/finalize \
    && mv \
      /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth \
      /usr/local/share/whoscored/build-hooks/bootstrap \
    && mv \
      /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth \
      /usr/local/share/whoscored/build-hooks/finalize \
    && test ! -e /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth \
    && test ! -e /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth
COPY --chown=root:root whoscored-runtime-trust-root-production /usr/local/share/whoscored/runtime-trust-root
RUN --network=none chmod 0444 /usr/local/share/whoscored/runtime-trust-root
RUN --network=none /usr/local/bin/python -S -m venv /opt/legacy-scraper-venv \
    && chown -R airflow:root /opt/legacy-scraper-venv \
    && chown -R airflow:root /home/airflow/.local \
    && install -d -o airflow -g root -m 0755 \
        /home/airflow/.cache \
        /home/airflow/soccerdata

USER airflow
RUN curl -fsSL https://files.pythonhosted.org/packages/1f/98/7ab46625ce2317756e4e857fe6ac24b6378c3e8f477da26c95226ed8ccb7/undetected-chromedriver-3.5.5.tar.gz -o /tmp/undetected-chromedriver-3.5.5.tar.gz \
    && echo "9f945e1435005247abe17de316bcfda85b284a4177fd5f25167c78ced33b65ec  /tmp/undetected-chromedriver-3.5.5.tar.gz" \
        | sha256sum -c - \
    && test "$(stat -c %s /tmp/undetected-chromedriver-3.5.5.tar.gz)" -eq 65409
RUN --network=none SOURCE_DATE_EPOCH=1700000000 PYTHONHASHSEED=0 \
    PYTHONPATH=/usr/local/lib/python3.11/site-packages:/home/airflow/.local/lib/python3.11/site-packages \
    /usr/local/bin/python -S -m pip wheel \
      --disable-pip-version-check --no-deps --no-build-isolation \
      --wheel-dir /tmp/whoscored-wheelhouse \
      /tmp/undetected-chromedriver-3.5.5.tar.gz \
    && echo "3f3843d0ae6e26ad12df5296293c863595edc4a62f348348ccb698433da44960  /tmp/whoscored-wheelhouse/undetected_chromedriver-3.5.5-py3-none-any.whl" \
        | sha256sum -c - \
    && test "$(stat -c %s /tmp/whoscored-wheelhouse/undetected_chromedriver-3.5.5-py3-none-any.whl)" -eq 47131
RUN PYTHONUSERBASE=/home/airflow/.local \
    PYTHONPATH=/usr/local/lib/python3.11/site-packages:/home/airflow/.local/lib/python3.11/site-packages \
    /usr/local/bin/python -S -m pip install \
      --no-cache-dir --user --no-deps --require-hashes --only-binary=:all: \
      -r /tmp/requirements-scheduler.txt
RUN --network=none test "$(sed -n 's/^include-system-site-packages = //p' \
        /opt/legacy-scraper-venv/pyvenv.cfg)" = false \
    && /opt/legacy-scraper-venv/bin/python -I -c \
        "import site,sys,sysconfig; assert sys.prefix == '/opt/legacy-scraper-venv'; assert sys.base_prefix == '/usr/local'; assert site.ENABLE_USER_SITE is False; assert sysconfig.get_path('purelib') == '/opt/legacy-scraper-venv/lib/python3.11/site-packages'; assert '/usr/local/lib/python3.11/site-packages' not in sys.path"
RUN PIP_FIND_LINKS=/tmp/whoscored-wheelhouse \
    PIP_REQUIRE_VIRTUALENV=1 \
    /opt/legacy-scraper-venv/bin/python -I -m pip install \
      --no-cache-dir --no-deps --require-hashes --only-binary=:all: \
      -r /tmp/requirements-scraper-runner.txt

# The Python package's `fetch` command selects a mutable "latest compatible"
# GitHub release. Fetch each source's exact supported browser asset instead and
# verify its digest and byte size before offline extraction. SofaScore's paid
# runtime remains on the measured v135 browser in its package-default cache;
# FBref's v152 browser lives only in its explicit /opt tree.
RUN curl --proto '=https' --tlsv1.2 --proto-redir '=https' -fsSL \
      "https://github.com/daijro/camoufox/releases/download/v135.0.1-beta.24/camoufox-135.0.1-beta.24-lin.x86_64.zip" \
      -o /tmp/sofascore-camoufox.zip \
    && echo "61e1ec455e021720af38a5cc5ff7566121363cb5b82b72f24e381ba2676a4888  /tmp/sofascore-camoufox.zip" \
        | sha256sum -c - \
    && test "$(stat -c %s /tmp/sofascore-camoufox.zip)" -eq 712711368
RUN --network=none rm -rf /home/airflow/.cache/camoufox \
    && install -d -m 0755 /home/airflow/.cache/camoufox \
    && /usr/local/bin/python -S -m zipfile \
      -e /tmp/sofascore-camoufox.zip \
      /home/airflow/.cache/camoufox \
    && printf '%s\n' \
      "{\"version\":\"135.0.1\",\"release\":\"beta.24\"}" \
      > /home/airflow/.cache/camoufox/version.json \
    && chmod -R 0755 /home/airflow/.cache/camoufox \
    && test -x /home/airflow/.cache/camoufox/camoufox-bin \
    && rm -f /tmp/sofascore-camoufox.zip

RUN curl --proto '=https' --tlsv1.2 --proto-redir '=https' -fsSL \
      "https://github.com/daijro/camoufox/releases/download/v152.0.4-beta.26/camoufox-152.0.4-beta.26-lin.x86_64.zip" \
      -o /tmp/fbref-camoufox.zip \
    && echo "a03872a221ab766f58d04fdaf0d7f3431c2662d5086844c67d2fc01154ebc1f8  /tmp/fbref-camoufox.zip" \
        | sha256sum -c - \
    && test "$(stat -c %s /tmp/fbref-camoufox.zip)" -eq 663773735
RUN --network=none rm -rf \
      /tmp/undetected-chromedriver-3.5.5.tar.gz \
      /tmp/whoscored-wheelhouse

USER root
RUN --network=none rm -rf /opt/fbref-camoufox \
    && install -d -o root -g root -m 0755 /opt/fbref-camoufox \
    && /usr/local/bin/python -S -m zipfile \
      -e /tmp/fbref-camoufox.zip /opt/fbref-camoufox \
    && printf '%s\n' \
      "{\"version\":\"152.0.4\",\"release\":\"beta.26\"}" \
      > /opt/fbref-camoufox/version.json \
    && chown -R root:root /opt/fbref-camoufox \
    && chmod -R 0755 /opt/fbref-camoufox \
    && test -x /opt/fbref-camoufox/camoufox-bin \
    && test -r /opt/fbref-camoufox/fontconfig/windows/fonts.conf \
    && rm -f /tmp/fbref-camoufox.zip
RUN --network=none install -d -o root -g root -m 0755 /opt/tls-client
RUN curl --proto '=https' --tlsv1.2 --proto-redir '=https' -fsSL https://github.com/bogdanfinn/tls-client/releases/download/v1.13.1/tls-client-linux-ubuntu-amd64-1.13.1.so -o /opt/tls-client/tls-client.so \
    && echo "e4a4a5d771d1bd1558186a5ee46af1dfa1318bd31950d68ccd81ed30bad078fc  /opt/tls-client/tls-client.so" \
        | sha256sum -c - \
    && test "$(stat -c %s /opt/tls-client/tls-client.so)" -eq 15236872
RUN --network=none chmod 0444 /opt/tls-client/tls-client.so
ENV TLS_LIBRARY_PATH=/opt/tls-client/tls-client.so

USER airflow
# BuildKit's RUN sandbox cannot provide the user namespaces and graphics
# runtime needed by this Firefox build, even in native headless mode.  The
# downloaded asset is still digest-, size-, version-, and executable-checked
# above. CI launches that exact binary in virtual-headless mode from the
# finished, production-shaped container before any image can be promoted.
RUN --network=none PYTHONPATH=/usr/local/lib/python3.11/site-packages:/home/airflow/.local/lib/python3.11/site-packages \
    /usr/local/bin/python -S -c "import tls_requests; client = tls_requests.Client(proxy='http://127.0.0.1:9'); client.close()"
RUN --network=none PYTHONPATH=/usr/local/lib/python3.11/site-packages:/home/airflow/.local/lib/python3.11/site-packages \
      /usr/local/bin/python -S -m pip check \
    && PIP_REQUIRE_VIRTUALENV=1 \
      /opt/legacy-scraper-venv/bin/python -I -m pip check \
    && PYTHONPATH=/usr/local/lib/python3.11/site-packages:/home/airflow/.local/lib/python3.11/site-packages \
      PYTHONWARNINGS=error /usr/local/bin/python -S -c "import importlib.metadata as m; import requests; assert m.version('chardet') == '5.2.0'" \
    && /opt/legacy-scraper-venv/bin/python -I -c "import importlib.metadata as m; import camoufox, playwright, selenium, seleniumbase, soccerdata, urllib3; assert m.version('rich') == '14.3.4'; assert m.version('wrapper-tls-requests') == '1.2.5'; assert urllib3.__version__.startswith('1.')"
# No production task may alter an image dependency or plant a user-site .pth
# for the next interpreter.  Installation is complete, so freeze every
# user-owned Python/browser tree back to root ownership before runtime.
USER root
RUN --network=none chown -R root:root \
        /home/airflow/.local \
        /home/airflow/.cache/camoufox \
        /opt/legacy-scraper-venv \
    && chown root:root /home/airflow /home/airflow/.cache \
    && chmod 0755 /home/airflow /home/airflow/.cache \
    && chmod -R u=rwX,go=rX \
        /home/airflow/.local \
        /home/airflow/.cache/camoufox \
        /opt/legacy-scraper-venv
RUN --network=none test "$(stat -c '%u:%g:%a' /usr/local/share/whoscored/build-hooks)" = "0:0:755" \
    && test "$(stat -c '%u:%g:%a:%h' /usr/local/share/whoscored/build-hooks/bootstrap)" = "0:0:444:1" \
    && test "$(stat -c '%u:%g:%a:%h' /usr/local/share/whoscored/build-hooks/finalize)" = "0:0:444:1" \
    && rm -rf /__whoscored_runtime_bytecode_disabled__ \
    && test ! -e /__whoscored_runtime_bytecode_disabled__ \
    && test ! -e /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth \
    && test ! -e /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth \
    && mv \
      /usr/local/share/whoscored/build-hooks/bootstrap \
      /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth \
    && mv \
      /usr/local/share/whoscored/build-hooks/finalize \
      /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth \
    && rmdir /usr/local/share/whoscored/build-hooks \
    && chmod 0444 \
      /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth \
      /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth \
    && test "$(stat -c '%u:%g:%a:%h' /usr/local/lib/python3.11/site-packages/00000000-whoscored-runtime-bootstrap.pth)" = "0:0:444:1" \
    && test "$(stat -c '%u:%g:%a:%h' /usr/local/lib/python3.11/site-packages/zzzzzzzz-whoscored-runtime-finalize.pth)" = "0:0:444:1" \
    && test "$(find /usr/local/lib/python3.11/site-packages -maxdepth 1 \
        -type f -name '*.pth' -printf '%f\n' | LC_ALL=C sort | head -n 1)" \
      = "00000000-whoscored-runtime-bootstrap.pth" \
    && test "$(find /usr/local/lib/python3.11/site-packages -maxdepth 1 \
        -type f -name '*.pth' -printf '%f\n' | LC_ALL=C sort | tail -n 1)" \
      = "zzzzzzzz-whoscored-runtime-finalize.pth"

# Debian Chromium's crash handler needs a writable config root even when the
# browser itself has an explicit --user-data-dir. Keep HOME sealed and route
# only Chromium's disposable state to the container-local temporary tree.
ENV CHROME_CONFIG_HOME=/tmp/airflow-chromium-config
USER airflow

WORKDIR /opt/airflow

# The dedicated WhoScored proxy has no browser/legacy runner dependencies, but
# it must carry the same production-class runtime trust root as the scheduler.
# Keep this self-reference-free payload stage distinct from the final gate
# layer so build provenance can bind its immutable image ID.
FROM airflow-base AS airflow-whoscored-proxy-payload
USER root
COPY --chown=root:root whoscored-runtime-trust-root-production /usr/local/share/whoscored/runtime-trust-root
RUN --network=none chmod 0444 /usr/local/share/whoscored/runtime-trust-root
USER airflow
WORKDIR /opt/airflow

# CI gets the production dependency/hardening shape without claiming the
# production authorization class consumed by paid WhoScored transports.
FROM airflow-scheduler-payload AS airflow-scheduler-test
USER root
COPY --chown=root:root whoscored-runtime-trust-root-test /usr/local/share/whoscored/runtime-trust-root
RUN --network=none chmod 0444 /usr/local/share/whoscored/runtime-trust-root
USER airflow
WORKDIR /opt/airflow

# The final dedicated proxy target is deliberately unusable while the
# checked-in evidence remains blocked-v1. Promotion replaces the two evidence
# inputs at build time only after the payload image and external final digest
# have been attested; neither file is a runtime mount or environment override.
FROM airflow-whoscored-proxy-payload AS airflow-whoscored-proxy
USER root
COPY --chown=root:root whoscored_production_gate.py /usr/local/libexec/whoscored_production_gate.py
COPY --chown=root:root whoscored-production-gate /usr/local/bin/whoscored-production-gate
COPY --chown=root:root whoscored-production-entrypoint /usr/local/bin/whoscored-production-entrypoint
COPY --chown=root:root whoscored-production-python /usr/local/bin/whoscored-production-python
COPY --chown=root:root whoscored-build-provenance-attestation.json /usr/local/share/whoscored/build-provenance-attestation.json
COPY --chown=root:root whoscored-build-provenance-manifest.json /usr/local/share/whoscored/build-provenance-manifest.json
RUN --network=none chmod 0444 \
        /usr/local/libexec/whoscored_production_gate.py \
        /usr/local/share/whoscored/build-provenance-attestation.json \
        /usr/local/share/whoscored/build-provenance-manifest.json \
    && chmod 0555 \
        /usr/local/bin/whoscored-production-gate \
        /usr/local/bin/whoscored-production-entrypoint \
        /usr/local/bin/whoscored-production-python \
    && mv /usr/local/bin/python3.11 /usr/local/libexec/whoscored-python-real \
    && chown root:root /usr/local/libexec/whoscored-python-real \
    && chmod 0555 /usr/local/libexec/whoscored-python-real \
    && install -o root -g root -m 0555 \
        /usr/local/bin/whoscored-production-python /usr/local/bin/python3.11 \
    && rm -f /usr/local/bin/python /usr/local/bin/python3 \
    && ln -s python3.11 /usr/local/bin/python3 \
    && ln -s python3 /usr/local/bin/python
USER 50000:0
WORKDIR /opt/airflow
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/usr/local/bin/whoscored-production-entrypoint", "/entrypoint"]

# Scheduler final stage. Before replacing the system interpreter, materialize
# one venv-local executable so the isolated legacy runner keeps its pyvenv.cfg
# identity instead of following the new system gate wrapper. That raw legacy
# interpreter is outside PATH and is part of the trusted Docker-host boundary;
# WhoScored production runners always use the gated system alias.
FROM airflow-scheduler-payload AS airflow-scheduler
USER root
COPY --chown=root:root whoscored_production_gate.py /usr/local/libexec/whoscored_production_gate.py
COPY --chown=root:root whoscored-production-gate /usr/local/bin/whoscored-production-gate
COPY --chown=root:root whoscored-production-entrypoint /usr/local/bin/whoscored-production-entrypoint
COPY --chown=root:root whoscored-production-python /usr/local/bin/whoscored-production-python
COPY --chown=root:root whoscored-build-provenance-attestation.json /usr/local/share/whoscored/build-provenance-attestation.json
COPY --chown=root:root whoscored-build-provenance-manifest.json /usr/local/share/whoscored/build-provenance-manifest.json
RUN --network=none cp --dereference /opt/legacy-scraper-venv/bin/python3.11 \
        /tmp/whoscored-legacy-python3.11 \
    && test "$(stat -c '%u:%g:%a:%h' /usr/local/libexec/whoscored_capacity_worker_bootstrap.py)" = "0:0:444:1" \
    && rm -f \
        /opt/legacy-scraper-venv/bin/python \
        /opt/legacy-scraper-venv/bin/python3 \
        /opt/legacy-scraper-venv/bin/python3.11 \
    && install -o root -g root -m 0555 \
        /tmp/whoscored-legacy-python3.11 \
        /opt/legacy-scraper-venv/bin/python3.11 \
    && rm -f /tmp/whoscored-legacy-python3.11 \
    && ln -s python3.11 /opt/legacy-scraper-venv/bin/python3 \
    && ln -s python3 /opt/legacy-scraper-venv/bin/python \
    && chmod 0444 \
        /usr/local/libexec/whoscored_production_gate.py \
        /usr/local/share/whoscored/build-provenance-attestation.json \
        /usr/local/share/whoscored/build-provenance-manifest.json \
    && chmod 0555 \
        /usr/local/bin/whoscored-production-gate \
        /usr/local/bin/whoscored-production-entrypoint \
        /usr/local/bin/whoscored-production-python \
    && mv /usr/local/bin/python3.11 /usr/local/libexec/whoscored-python-real \
    && chown root:root /usr/local/libexec/whoscored-python-real \
    && chmod 0555 /usr/local/libexec/whoscored-python-real \
    && install -o root -g root -m 0555 \
        /usr/local/bin/whoscored-production-python /usr/local/bin/python3.11 \
    && rm -f /usr/local/bin/python /usr/local/bin/python3 \
    && ln -s python3.11 /usr/local/bin/python3 \
    && ln -s python3 /usr/local/bin/python
USER 50000:0
WORKDIR /opt/airflow
ENTRYPOINT ["/usr/bin/dumb-init", "--", "/usr/local/bin/whoscored-production-entrypoint", "/entrypoint"]
