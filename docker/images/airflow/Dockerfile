FROM apache/airflow:2.11.2-python3.11

USER root

# Install Java and system dependencies
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jdk-headless \
    procps \
    curl \
    netcat-openbsd \
    git \
    && rm -rf /var/lib/apt/lists/*

# Chrome/Chromium for Selenium (headless browser automation)
# Xvfb for virtual display (bypasses Cloudflare headless detection)
# Mesa software-GL (libgl1-mesa-dri/glx/egl): gives Chromium 120 a real (non-null)
# WebGL context under Xvfb — Cloudflare reads a null WebGL context as a bot marker (#574)
RUN apt-get update && apt-get install -y --no-install-recommends \
    chromium \
    chromium-driver \
    libgl1-mesa-dri \
    libgl1-mesa-glx \
    libegl1-mesa \
    fonts-liberation \
    libasound2 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libgbm1 \
    libnspr4 \
    libnss3 \
    libxkbcommon0 \
    xdg-utils \
    xvfb \
    xauth \
    && rm -rf /var/lib/apt/lists/*

ENV CHROME_BIN=/usr/bin/chromium
ENV CHROMEDRIVER_PATH=/usr/bin/chromedriver
# Force software GL (llvmpipe) — no GPU in the container; avoids failed HW-GL probes (#574)
ENV LIBGL_ALWAYS_SOFTWARE=1

# Set Java environment (JDK retained for keytool / Java-based client deps)
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH=$JAVA_HOME/bin:$PATH

# Create directories
RUN mkdir -p /opt/airflow/dags /opt/airflow/logs /opt/airflow/scrapers

# Add custom paths to PYTHONPATH for DAG imports
ENV PYTHONPATH="/opt/airflow/dags:/opt/airflow/scrapers:/opt/airflow:${PYTHONPATH}"

# Switch to airflow user for pip installs
USER airflow

# ===== СТАДИЯ 1: Airflow 2.11.x (уже включен в базовый образ) =====
# Базовый образ apache/airflow:2.11.2-python3.11 уже содержит Airflow
# Дополнительная установка Airflow не требуется

# ===== СТАДИЯ 2: Core dependencies (Airflow providers, data processing) =====
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

# ===== СТАДИЯ 3: Scraping dependencies (installed separately to avoid conflicts) =====
COPY requirements-scraping.txt /tmp/requirements-scraping.txt
RUN pip install --no-cache-dir -r /tmp/requirements-scraping.txt

# ===== СТАДИЯ 4: Camoufox (anti-detect Firefox) binary for SofaScore (#757) =====
# Downloads the patched Firefox (~700MB) into /home/airflow/.cache/camoufox so the
# SofaScore Turnstile-bypass capture transport works at runtime (no per-task fetch).
# Runs as the airflow user (current USER) — the cache must live in its $HOME.
RUN python -m camoufox fetch

# Set working directory
WORKDIR /opt/airflow
