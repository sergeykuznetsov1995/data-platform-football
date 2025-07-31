from airflow import DAG
from airflow.providers.docker.operators.docker import DockerOperator
from datetime import datetime

with DAG(
    dag_id="fbref_parser_docker_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False,
) as dag:
    run_parser = DockerOperator(
        task_id="run_fbref_parser_container",
        image="data-platform-fbref-parser:latest",  # имя твоего образа
        auto_remove=False,
        command=None,  # если ENTRYPOINT уже прописан
        network_mode="data-platform_default",  # или другой, если нужно доступ к HDFS
        # volumes=[...],  # если нужны volume
    )