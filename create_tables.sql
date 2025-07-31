-- Создание схемы для данных FBRef
CREATE SCHEMA IF NOT EXISTS fbref.arsenal_data;

-- Основная таблица с детальной информацией игроков
CREATE TABLE fbref.arsenal_data.players_detailed (
    name VARCHAR,
    position VARCHAR,
    footed VARCHAR,
    birth VARCHAR,
    team VARCHAR,
    season VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/players_detailed'
);

-- Статистика команды
CREATE TABLE fbref.arsenal_data.team_stats (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET', 
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/team_stats'
);

-- Таблица Премьер лиги
CREATE TABLE fbref.arsenal_data.premier_league_table (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/premier_league_table'
);

-- Стандартная статистика
CREATE TABLE fbref.arsenal_data.standard_stats (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/standard_stats'
);

-- Продвинутая вратарская статистика
CREATE TABLE fbref.arsenal_data.advanced_goalkeeping (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/advanced_goalkeeping'
);

-- Защитные действия
CREATE TABLE fbref.arsenal_data.defensive_actions (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/defensive_actions'
);

-- Создание голов и ударов
CREATE TABLE fbref.arsenal_data.goal_and_shot_creation (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/goal_and_shot_creation'
);

-- Вратарская статистика
CREATE TABLE fbref.arsenal_data.goalkeeping (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/goalkeeping'
);

-- Разная статистика
CREATE TABLE fbref.arsenal_data.miscellaneous_stats (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/miscellaneous_stats'
);

-- Типы передач
CREATE TABLE fbref.arsenal_data.pass_types (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/pass_types'
);

-- Передачи
CREATE TABLE fbref.arsenal_data.passing (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/passing'
);

-- Время игры
CREATE TABLE fbref.arsenal_data.playing_time (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/playing_time'
);

-- Владение мячом
CREATE TABLE fbref.arsenal_data.possession (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/possession'
);

-- Удары
CREATE TABLE fbref.arsenal_data.shooting (
    team VARCHAR,
    season VARCHAR,
    competition VARCHAR,
    stats_type VARCHAR
) WITH (
    format = 'PARQUET',
    external_location = 'hdfs://namenode:9000/fbref/arsenal_data/shooting'
);

