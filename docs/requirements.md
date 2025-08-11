# Data Platform Architecture Requirements

## 🏗️ Общая архитектура системы

Платформа данных для футбольной аналитики на основе современного стека технологий больших данных.

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Data Sources  │    │   Processing    │    │   Analytics     │
│                 │    │                 │    │                 │
│  📊 FBRef.com   │───▶│  🔄 Airflow     │───▶│  🔍 Trino       │
│  ⚽ Football    │    │  ⚡ Spark       │    │  📊 SQL Queries │
│     Statistics  │    │  🐍 Python      │    │  📈 BI Tools    │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                │
                                ▼
                       ┌─────────────────┐
                       │   Data Storage  │
                       │                 │
                       │  📁 HDFS        │
                       │  🗄️ Hive Meta   │
                       │  💾 PostgreSQL  │
                       └─────────────────┘
```

## 🛠️ Компоненты системы

### 1. **Хранение данных (Data Storage Layer)**

#### 📁 HDFS (Hadoop Distributed File System)
- **Роль**: Распределенное хранилище больших данных
- **Компоненты**:
  - `namenode` - управляющий узел, хранит метаданные файловой системы
  - `datanode` - узел данных, хранит блоки файлов
- **Порты**:
  - 9870 - HDFS Web UI
  - 9000 - HDFS API
  - 9864 - DataNode Web UI
- **Volumes**: 
  - `namenode_data:/hadoop/dfs/name`
  - `datanode_data:/hadoop/dfs/data`

#### 💾 PostgreSQL
- **Роль**: База данных для Hive Metastore
- **Конфигурация**:
  - Database: `metastore`
  - User: `hive`
  - Password: `hivepassword`
- **Порт**: 5432
- **Volume**: `postgres_data:/var/lib/postgresql/data`

#### 🗄️ Hive Metastore
- **Роль**: Каталог метаданных для таблиц и схем
- **Функции**:
  - Хранение схем таблиц (колонки, типы данных)
  - Расположение файлов в HDFS
  - Метаданные форматов (Parquet, ORC, JSON)
  - Информация о партиционировании
- **Порт**: 9083
- **Зависимости**: PostgreSQL, HDFS NameNode

### 2. **Обработка данных (Data Processing Layer)**

#### ⚡ Apache Spark
- **Роль**: Движок для обработки больших данных
- **Режим**: Master node
- **Порты**:
  - 8080 - Spark Web UI
  - 7077 - Spark Master
- **Volume**: `spark_data:/bitnami/spark`
- **Использование**: Batch обработка, ETL процессы

#### 🔄 Apache Airflow
- **Роль**: Оркестрация и планирование задач
- **Executor**: LocalExecutor
- **Функции**:
  - Управление DAG (Directed Acyclic Graphs)
  - Планирование выполнения задач
  - Мониторинг пайплайнов
  - Docker операторы для запуска контейнеров
- **Порт**: 8082 (Airflow Web UI)
- **Volumes**:
  - `./airflow/dags:/opt/airflow/dags`
  - `./fbref_data:/opt/airflow/fbref_data`
  - `/var/run/docker.sock:/var/run/docker.sock`

### 3. **Аналитика (Analytics Layer)**

#### 🔍 Trino (ранее Presto)
- **Роль**: SQL движок для аналитических запросов
- **Возможности**:
  - Федеративные запросы к множественным источникам
  - Поддержка различных форматов (Parquet, ORC, JSON)
  - Высокопроизводительная обработка запросов
- **Порты**:
  - 8081 - Trino Web UI  
  - 8443 - Trino HTTPS
- **Каталоги**:
  - `fbref` - для футбольных данных
  - `memory` - для временных данных
- **Volume**: `./trino/etc:/etc/trino`

### 4. **Источники данных (Data Sources)**

#### ⚽ FBRef Parser
- **Роль**: Извлечение футбольных данных с FBRef.com
- **Технологии**:
  - Python + BeautifulSoup для парсинга
  - pandas для обработки данных
  - pyarrow для сохранения в Parquet
  - hdfs для интеграции с HDFS
- **Функции**:
  - Анти-блокировка (прогрессивные задержки, ротация User-Agent)
  - Автоматические retry с exponential backoff
  - Сохранение в HDFS с fallback на локальное хранение
- **Данные**:
  - Статистика команд (Arsenal 2024/2025)
  - Детальная информация об игроках
  - Метаданные парсинга

## 🌐 Сетевая архитектура

### Docker Network: `data-platform`
Все сервисы работают в единой Docker сети для обеспечения связности.

### Внешние порты:
- **5432** - PostgreSQL
- **8080** - Spark Web UI
- **8081** - Trino Web UI
- **8082** - Airflow Web UI  
- **9000** - HDFS API
- **9083** - Hive Metastore
- **9864** - DataNode Web UI
- **9870** - HDFS NameNode Web UI

## 📊 Структура данных

### HDFS организация:
```
/fbref/arsenal_2024_2025/
├── team_stats/
│   └── team_stats.parquet     # Статистика команды (38 записей)
└── players_detailed/
    └── players_detailed.parquet # Игроки (5 записей)
```

### Trino схемы:
```
fbref.arsenal_data.team_stats        # Командная статистика
fbref.arsenal_data.players_detailed  # Детальная информация игроков
```

## 🔄 Data Pipeline Workflow

### 1. Извлечение данных (Extract)
```
FBRef.com → Python Parser → Anti-blocking → Raw Data
```

### 2. Преобразование (Transform)  
```
Raw HTML → BeautifulSoup → pandas DataFrame → Data Cleaning
```

### 3. Загрузка (Load)
```
DataFrame → Parquet → HDFS → Hive Metastore → Trino Tables
```

### 4. Аналитика (Analytics)
```
Trino SQL → Business Intelligence → Insights
```

## 🚀 Deployment & Operations

### Запуск системы:
```bash
# Запуск всех сервисов
docker-compose up -d

# Проверка статуса
docker-compose ps
```

### Мониторинг:
- **Airflow UI**: http://localhost:8082 - состояние DAG и задач
- **Trino UI**: http://localhost:8081 - активные запросы и ресурсы  
- **Spark UI**: http://localhost:8080 - задачи Spark
- **HDFS UI**: http://localhost:9870 - состояние файловой системы

### Управление задачами:
```bash
# Пересборка парсера
cd fbref_parser && ./build.sh

# Настройка таблиц Trino
./setup_fbref.sh

# Запуск парсинга через Airflow
# DAG: fbref_simple_parser (ручной запуск)
```

## 🔧 Требования к системе

### Минимальные ресурсы:
- **RAM**: 8GB (рекомендуется 16GB)
- **CPU**: 4 cores (рекомендуется 8 cores)
- **Disk**: 50GB свободного места
- **Network**: Стабильное интернет-соединение для парсинга

### Программное обеспечение:
- Docker 20.10+
- Docker Compose 2.0+
- Linux/macOS (протестировано на Ubuntu 22.04)

## 📈 Масштабирование

### Горизонтальное масштабирование:
- Добавление DataNode для HDFS
- Spark Worker nodes для распределенной обработки
- Trino Worker nodes для параллельных запросов

### Вертикальное масштабирование:
- Увеличение memory для Trino (jvm.config)
- Больше CPU cores для Spark
- SSD диски для лучшей производительности HDFS

## 🛡️ Безопасность

### Текущая конфигурация (Development):
- **Аутентификация**: Отключена (NONE)
- **Шифрование**: HTTP (не HTTPS)
- **Доступ**: Открытый в Docker сети

### Production требования:
- Настройка HTTPS для всех UI
- Аутентификация для Trino (LDAP/OAuth)
- Файрвол правила для портов
- Backup стратегия для данных

## 📋 Примеры использования

### SQL Аналитика через Trino:
```sql
-- Топ бомбардиров Arsenal
SELECT name, position, CAST(stats_goals AS INTEGER) as goals
FROM fbref.arsenal_data.players_detailed 
WHERE stats_goals IS NOT NULL AND stats_goals != ''
ORDER BY goals DESC;

-- Статистика по позициям
SELECT position, COUNT(*) as players_count
FROM fbref.arsenal_data.players_detailed
GROUP BY position;
```

### Python анализ:
```python
import trino
conn = trino.dbapi.connect(
    host='localhost',
    port=8081,
    user='user'
)
cur = conn.cursor()
cur.execute("SELECT * FROM fbref.arsenal_data.players_detailed")
data = cur.fetchall()
```

## 🎯 Roadmap

### Краткосрочные цели:
- [ ] Добавить парсинг других команд Premier League
- [ ] Реализовать инкрементальные обновления
- [ ] Настроить автоматическое расписание в Airflow

### Долгосрочные цели:
- [ ] Интеграция с Grafana для дашбордов
- [ ] Machine Learning модели для предсказаний
- [ ] Real-time стриминг через Kafka
- [ ] Kubernetes деплой для production

---

**Архитектура обеспечивает**: масштабируемость, надежность, производительность и простоту использования для аналитики футбольных данных.
