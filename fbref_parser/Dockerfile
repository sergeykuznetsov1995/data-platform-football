FROM apache/airflow:2.8.1

# Переключаемся на root для установки зависимостей
USER root

# Устанавливаем Java, системные утилиты и зависимости для Playwright
RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    wget \
    curl \
    ca-certificates \
    # Зависимости для Playwright (полный набор)
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxrandr2 \
    libgbm1 \
    libxss1 \
    libasound2 \
    libxfixes3 \
    libpango-1.0-0 \
    libcairo2 \
    fonts-liberation \
    libasound2 \
    libatk1.0-0 \
    libcairo-gobject2 \
    libgtk-3-0 \
    libgdk-pixbuf2.0-0 \
    libglib2.0-0 \
    && rm -rf /var/lib/apt/lists/*

# Скачиваем и распаковываем Hadoop
ENV HADOOP_VERSION=3.3.6
RUN wget https://downloads.apache.org/hadoop/common/hadoop-${HADOOP_VERSION}/hadoop-${HADOOP_VERSION}.tar.gz && \
    tar -xzf hadoop-${HADOOP_VERSION}.tar.gz -C /opt && \
    rm hadoop-${HADOOP_VERSION}.tar.gz
ENV HADOOP_HOME=/opt/hadoop-${HADOOP_VERSION}
ENV PATH=$HADOOP_HOME/bin:$PATH
ENV ARROW_LIBHDFS_DIR=$HADOOP_HOME/lib/native

# Копируем requirements (пока под root)
COPY requirements.txt .

# Переключаемся на airflow для установки Python пакетов
USER airflow
RUN pip install --no-cache-dir -r requirements.txt

# Переключаемся на airflow для установки браузера Playwright
USER airflow
RUN playwright install chromium

# Копируем обновленный парсер
COPY main.py .

# Точка входа
CMD ["python", "main.py"]
